from typing import TYPE_CHECKING

if TYPE_CHECKING:
	type JSONSerializable = None | bool | int | float | str | list[JSONSerializable] | dict[str, JSONSerializable]

class EventManager:
	"""Event-style RPC implementation"""

	if TYPE_CHECKING:
		from concurrent.futures.thread import ThreadPoolExecutor
		from .communication import AbstractPacketStream
		from .packet_manager import PacketManager
		from .packet_manager import CloseState
		from typing import Literal, Callable
		from threading import Lock

	_packet_manager: PacketManager

	_executor: ThreadPoolExecutor

	_bound_procedures_lock: Lock
	_bound_procedures: dict[str, Callable[..., None]]

	def __init__(self, stream: AbstractPacketStream, call_timeout: float = 3.0, max_concurrent_calls: int = 4):
		"""
			Create a new EventManager instance.

			:param stream: The packet stream to use as transport.
			:param call_timeout: The timeout for sending calls.
			:param max_concurrent_calls: The number of threads for the call executor.
		"""

		from concurrent.futures.thread import ThreadPoolExecutor
		from .packet_manager import PacketManager
		from threading import Lock

		self._packet_manager = PacketManager(stream, call_timeout, call_timeout)
		self._packet_manager.bind_event("packet", self._on_packet)

		self._executor = ThreadPoolExecutor(max_concurrent_calls)

		self._bound_procedures_lock = Lock()
		self._bound_procedures = {}


	def _on_call(self, procedure: Callable, procedure_name: str, positional_arguments, keyword_arguments):
		from warnings import warn

		try:
			self._executor.submit(procedure, *positional_arguments, **keyword_arguments)
		except BaseException as exception:
			warn(f"Received call for procedure \"{procedure_name}\" failed with {exception.__class__.__name__}: {exception}")

	def _on_packet(self, packet: bytes):
		from warnings import warn
		from json import loads

		call: dict[str, JSONSerializable] = loads(packet)

		with self._bound_procedures_lock:
			if call.get("procedure_name") in self._bound_procedures:
				procedure = self._bound_procedures[call["procedure_name"]]
			else:
				warn(f"Received call for unbound procedure \"{call['procedure_name']}\"")

				return

		self._executor.submit(self._on_call, procedure, call["procedure_name"], call["positional_arguments"], call["keyword_arguments"])


	def bind_event(self, event: Literal["close"], callback: Callable[[CloseState], None]):
		"""
			Bind a function to an event.

			:param event: The event to bind to.
			:param callback: The function to bind.
		"""

		self._packet_manager.bind_event(event, callback)

	def bind(self, procedure: Callable[..., None], name: str | None = None):
		"""
			Bind a procedure for remote calling.

			:param procedure: The procedure to bind.
			:param name: The name under which the procedure will be bound.

			:return: The procedure, for decorator compatibility.
		"""

		if name is None:
			name = procedure.__name__

		with self._bound_procedures_lock:
			self._bound_procedures[name] = procedure

		return procedure

	def call(self, name: str, *positional_arguments, **keyword_arguments):
		"""
			Queue a remote procedure call.

			For exception traces and errors, check remote application logs.

			:param name: The name of the procedure.
			:param positional_arguments: Positional arguments, passed to the procedure.
			:param keyword_arguments: Keyword arguments, passed to the procedure.

			:raises ManagerClosed: If the manager is closed at the time of queueing.
		"""

		from json import dumps

		data = {
			"procedure_name": name,
			"positional_arguments": positional_arguments,
			"keyword_arguments" : keyword_arguments
		}

		self._packet_manager.send_packet(dumps(data).encode())

	def close(self, wait_futures: bool = True):
		"""
			Close the EventManager and optionally wait for calls to finish.

			:param wait_futures: If set to true, the event manager will wait for currently executing calls to finish before closing.
		"""

		self._executor.shutdown(wait = wait_futures)
		self._packet_manager.close()

	def join(self):
		"""
			Wait for stream close and return state.

			:return CloseState: The state of the stream worker thread at close.
		"""

		return self._packet_manager.join()