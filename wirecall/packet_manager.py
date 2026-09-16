from typing import TYPE_CHECKING
from enum import Enum


class ManagerClosed(Exception):
	"""The manager has been closed"""

class CloseState(Enum):
	"""The way the Packet Manager closed"""

	OK = 0
	ABRUPT = 1
	EXCEPTION = 2

class PacketManager:
	"""Thread-safe packet stream wrapper"""

	if TYPE_CHECKING:
		from .communication import AbstractPacketStream
		from typing import Callable, Literal
		from threading import Lock, Event
		from queue import Queue

	_stream: AbstractPacketStream
	_send_timeout: float
	_receive_timeout: float
	_send_queue: Queue[bytes]

	_event_bindings_lock: Lock
	_event_bindings: dict[str, list[Callable]]

	_closing_event: Event
	_close_result: Queue[CloseState]

	def __init__(self, stream: AbstractPacketStream, send_timeout: float = 4.0, receive_timeout: float = 4.0):
		"""
			Create a new packet manager.

			A packet manager is a thread-safe interface to a packet stream.

			:param stream: The packet stream.
			:param send_timeout: The timeout for sending packets.
			:param receive_timeout: The timeout for receiving packets.
		"""

		from threading import Thread, Lock, Event
		from queue import Queue
		from os import pipe

		command_pipe_read, command_pipe_write = pipe()

		self._stream = stream
		self._send_timeout = send_timeout
		self._receive_timeout = receive_timeout
		self._send_queue = Queue(64)

		self._event_bindings_lock = Lock()
		self._event_bindings = {"stop": [], "packet": []}

		self._close_result = Queue(1)
		self._closing_event = Event()
		self._command_pipe = command_pipe_write

		self._thread_object = Thread(
			target = self._worker,
			args = (command_pipe_read, ),
			name = "Packet Manager Thread"
		)

		self._thread_object.start()

	def _worker(self, command_file_descriptor):
		from .communication import PacketUnavailable, StreamEnded, StreamAbruptlyEnded, StreamClosed
		from os import read, close
		from warnings import warn
		from select import select

		stream_file_descriptor = self._stream.get_file_descriptor()
		close_state = CloseState.OK

		try:
			while True:
				read_ready, _, _ = select((stream_file_descriptor, command_file_descriptor), (), ())

				if stream_file_descriptor in read_ready:
					try:
						raw_packet = self._stream.recv_packet(self._receive_timeout, self._receive_timeout, True)
						packet_data = raw_packet[1:]
						packet_type = raw_packet[0]

						if packet_type == 0: # Request close
							self._stream.send_packet(b"\001")

							raise StreamEnded("Requested stream close")
						elif packet_type == 1: # Close request reply
							raise StreamEnded("Requested stream close completed")
						elif packet_type == 16: # Packet
							with self._event_bindings_lock:
								for bound_callable in self._event_bindings["packet"]:
									try:
										bound_callable(packet_data)
									except BaseException as exception:
										from traceback import format_exception

										warn(f"The function bound to the close event raised an exception: {format_exception(exception)}")
						else:
							raise ValueError(f"Received invalid packet type {packet_type}")
					except PacketUnavailable:
						pass

				if command_file_descriptor in read_ready:
					command = read(command_file_descriptor, 1)[0]

					if command == 0: # Request close
						self._stream.send_packet(b"\000")
					if command == 1: # Force close
						raise StreamEnded("Force closed")
					elif command == 2: # Send packet
						packet_data = self._send_queue.get()

						self._stream.send_packet(b"\020" + packet_data)
		except StreamAbruptlyEnded:
			close_state = CloseState.ABRUPT
		except (StreamEnded, StreamClosed):
			pass
		except BaseException:
			close_state = CloseState.EXCEPTION

			raise
		finally:
			self._closing_event.set()

			close(command_file_descriptor)

			self._stream.close()
			self._close_result.put(close_state)

			with self._event_bindings_lock:
				for bound_callable in self._event_bindings["close"]:
					try:
						bound_callable(close_state)
					except BaseException as exception:
						from traceback import format_exception

						warn(f"The function bound to the close event raised an exception: {format_exception(exception)}")

	def bind_event(self, event: Literal["close", "packet"], callback: Callable[[CloseState], None] | Callable[[bytes], None]) -> None:
		"""
			Bind a function to an event.

			:param event: The event to bind to.
			:param callback: The function to bind.
		"""

		with self._event_bindings_lock:
			if event not in self._event_bindings:
				self._event_bindings[event] = []

			self._event_bindings[event].append(callback)

	def unbind(self, event: Literal["close", "packet"], callback: Callable[[], None] | Callable[[bytes], None]) -> None:
		"""
			Unbind a function from an event.

			:param event: The event to unbind from.
			:param callback: The function to unbind.
		"""

		with self._event_bindings_lock:
			if event not in self._event_bindings:
				return

			event_bindings = self._event_bindings[event]

			if callback in event_bindings:
				event_bindings.remove(callback)

	def send_packet(self, packet: bytes) -> None:
		"""
			Queue a packet to be sent.

			This call does not guarantee the sending of the packet.

			:param packet: The packet to send.

			:raises ManagerClosed: If the manager is closed at the time of queueing.
		"""

		from os import write

		if self._closing_event.is_set():
			raise ManagerClosed("The manager is closed")

		self._send_queue.put(packet)
		write(self._command_pipe, b"\002")

	def join(self, timeout: float | None = None) -> CloseState:
		"""
			Wait for stream close and return state.

			:return CloseState: The state of the worker thread at close.
		"""

		from queue import Empty


		self._thread_object.join(timeout)

		if self._thread_object.is_alive():
			raise TimeoutError("The manager did not close in time")

		try:
			return self._close_result.get_nowait()
		except Empty:
			return CloseState.EXCEPTION

	def close(self, timeout: float | None = None, force: bool = False) -> None:
		"""
			Close the stream and manager.

			:param timeout: The timeout for waiting. Set to zero to avoid waiting.
			:param force: If set to true, an unclean close will be requested. Useful for stuck streams.
		"""

		from os import write, close

		self._closing_event.set()

		write(self._command_pipe, b"\001" if force else b"\000")

		self._thread_object.join(timeout)

		if self._thread_object.is_alive():
			raise TimeoutError("The manager did not close in time")

		close(self._command_pipe)