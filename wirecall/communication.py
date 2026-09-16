from typing import TYPE_CHECKING


# region Abstract interface

from abc import ABC, abstractmethod

# region Errors

class StreamError(IOError):
	"""General stream error"""

class StreamAbruptlyEnded(StreamError):
	"""Raised when the stream end is reached under illegal conditions"""

class StreamEnded(StreamError):
	"""Raised when the stream end is reached under normal conditions"""

class StreamClosed(StreamError):
	"""Raised when the stream cannot be written to as a result of it being closed"""

class PacketUnavailable(StreamError):
	"""Raised when no bytes are immediately available to be read"""

# endregion

class AbstractPacketStream(ABC):
	"""Packet stream abstract skeleton"""

	packet_max_size: int = 1024 * 1024

	@abstractmethod
	def recv_packet(self, header_timeout: float | None = 1.0, content_timeout: float | None = 2.0, immediate: bool = False) -> bytes:
		"""
			Receive a packet from the stream.

			This operation may block.

			:param header_timeout: The timeout for each read operation while reading the header.
			:param content_timeout: The timeout for each read operation while reading the content.
			:param immediate: If set to True and no data is immediately available to be read, the call will raise PacketUnavailable.

			:raises StreamExhaustedError: When the stream end is reached while reading a packet.
			:raises StreamEnded: If the stream end is reached under normal conditions.
			:raises PacketUnavailable: When no bytes are stored in the stream buffers and as a result, no packet is immediately available. Only raised when immediate is True.
			:raises TimeoutError: If the packet could not be read in time.
			:raises ValueError: If the packet is too big.

			:return bytes: The packet contents.
		"""

		pass

	@abstractmethod
	def send_packet(self, data: bytes, send_timeout: float | None = 2.0) -> None:
		"""
			Send a packet down the stream.

			This operation may block.

			:param data: The packet contents.
			:param send_timeout: The timeout for each write operation.

			:raises StreamClosedError: If any of the write operations fail.
			:raises TimeoutError: If the packet could not be written in time.
			:raises ValueError: If the packet is too big.
		"""

		pass

	@abstractmethod
	def close(self) -> None:
		"""
			Closes the underlying file descriptor.
		"""

		pass

	@abstractmethod
	def get_file_descriptor(self) -> int:
		"""
			Get the file descriptor for polling.

			:return int: The file descriptor.
		"""

# endregion


class TCPSocketPacketStream(AbstractPacketStream):
	"""Packet stream implementation for TCP / TLS Sockets"""

	if TYPE_CHECKING:
		from socket import socket as Socket
		from ssl import SSLSocket

	_socket: Socket | SSLSocket
	_is_ssl_socket: bool

	def __init__(self, socket: Socket, is_ssl_socket: bool = False):
		self._is_ssl_socket = is_ssl_socket
		self._socket = socket

	def recv_packet(self, header_timeout: float | None = 1.0, content_timeout: float | None = 2.0, immediate: bool = False) -> bytes:
		from time import monotonic
		from struct import unpack

		# region Header reading

		initial_time = monotonic()
		header_buffer = bytearray()

		while len(header_buffer) < 4:
			if header_timeout is not None:
				remaining_timeout = header_timeout - monotonic() + initial_time

				if remaining_timeout <= 0:
					raise TimeoutError("Timed out")

				self._socket.settimeout(remaining_timeout)
			else:
				self._socket.settimeout(None)

			if len(header_buffer) == 0 and immediate:
				self._socket.setblocking(False)

			try:
				read_buffer = self._socket.recv(4 - len(header_buffer))
			except BlockingIOError:
				raise PacketUnavailable("No packet is available")
			except TimeoutError:
				raise
			except OSError as exception:
				raise StreamError("An error occurred") from exception

			if len(read_buffer) == 0:
				if len(header_buffer) == 0:
					raise StreamEnded("The stream ended")

				raise StreamAbruptlyEnded("The stream ended while reading the packet header")

			header_buffer.extend(read_buffer)

		packet_length = unpack("!I", bytes(header_buffer))[0]

		if packet_length > self.packet_max_size:
			raise ValueError("The packet is too big")

		# endregion

		# region Content reading

		initial_time = monotonic()
		packet_buffer = bytearray()

		while len(packet_buffer) < packet_length:
			if content_timeout is not None:
				remaining_timeout = content_timeout - monotonic() + initial_time

				if remaining_timeout <= 0:
					raise TimeoutError("Timed out")

				self._socket.settimeout(remaining_timeout)
			else:
				self._socket.settimeout(None)

			try:
				read_buffer = self._socket.recv(packet_length - len(packet_buffer))
			except TimeoutError:
				raise
			except OSError as exception:
				raise StreamError("An error occurred") from exception

			if len(read_buffer) == 0:
				raise StreamAbruptlyEnded("The stream ended while reading the packet body")

			packet_buffer.extend(read_buffer)

		# endregion

		return bytes(packet_buffer)

	def send_packet(self, data: bytes, send_timeout: float | None = 2.0) -> None:
		from struct import pack

		if len(data) > self.packet_max_size:
			raise ValueError("The packet is too big")

		packet = pack("!I", len(data)) + data

		self._socket.settimeout(send_timeout)

		try:
			self._socket.sendall(bytes(packet))
		except TimeoutError:
			raise
		except OSError as exception:
			raise StreamClosed("The stream was closed") from exception

	def get_file_descriptor(self) -> int:
		return self._socket.fileno()

	def close(self) -> None:
		self._socket.close()

class UDPSocketPacketStream(AbstractPacketStream):
	"""Packet stream implementation for UDP packets"""

	if TYPE_CHECKING:
		from socket import socket as Socket

	_socket: Socket

	packet_max_size = 1200

	def __init__(self, socket: Socket, remote_address: tuple[str, int] | None = None):
		self._socket = socket

		if remote_address:
			socket.connect(remote_address)

	def recv_packet(self, header_timeout: float | None = 1.0, content_timeout: float | None = 2.0, immediate: bool = False) -> bytes:
		if immediate:
			self._socket.settimeout(0.0)
		else:
			self._socket.settimeout(header_timeout)

		try:
			packet = self._socket.recv(self.packet_max_size + 16)
		except BlockingIOError:
			raise PacketUnavailable("No packet is available")
		except TimeoutError:
			raise
		except OSError as exception:
			from errno import EBADF, ECONNRESET

			if exception.errno in (EBADF, ECONNRESET):
				raise StreamEnded("The stream ended")

			raise StreamError("An error occurred") from exception

		if len(packet) > self.packet_max_size:
			raise ValueError("The packet is too big")

		return packet

	def send_packet(self, data: bytes, send_timeout: float | None = 2.0) -> None:
		from time import monotonic

		if len(data) > self.packet_max_size:
			raise ValueError("The packet is too big")

		self._socket.settimeout(send_timeout)

		initial_time = monotonic()

		while True:
			try:
				self._socket.send(data)

				break
			except TimeoutError:
				raise
			except OSError as exception:
				from errno import ENOBUFS, EAGAIN, EWOULDBLOCK
				from select import select
				from time import sleep

				if exception.errno in (ENOBUFS, EAGAIN, EWOULDBLOCK):
					if send_timeout is not None:
						remaining = send_timeout - (monotonic() - initial_time)

						if remaining <= 0:
							raise TimeoutError("Timed out")

						select_timeout = remaining
					else:
						select_timeout = None

					_, writable, _ = select((), (self._socket.fileno(), ), (), select_timeout)

					if not writable:
						raise TimeoutError("Timed out")
					else:
						continue
				else:
					raise StreamClosed("The stream was closed") from exception

	def close(self) -> None:
		self._socket.close()

	def get_file_descriptor(self) -> int:
		return self._socket.fileno()

class FilePacketStream(AbstractPacketStream):
	"""Packet stream implementation for file descriptors"""

	_file_descriptor: int

	def __init__(self, file_descriptor: int):
		"""
			Create a new packet stream for the given non-blocking file descriptor.

			If the file descriptor is blocking, it is set to non-blocking mode.

			:param file_descriptor: The file descriptor.
		"""
		from os import set_blocking

		self._file_descriptor = file_descriptor

		set_blocking(file_descriptor, False)

	def _read(self, count: int, timeout: float | None = None, do_immediate: bool = False) -> bytes:
		from time import monotonic
		from select import select
		from os import read

		start_time = monotonic()
		buffer = bytearray()

		while len(buffer) < count:
			try:
				try:
					read_buffer = read(self._file_descriptor, count - len(buffer))
				except BlockingIOError:
					if do_immediate and len(buffer) == 0:
						raise PacketUnavailable("No packet is available")

					raise
				except OSError:
					if len(buffer) == 0:
						raise StreamEnded("Stream end reached")

					raise StreamAbruptlyEnded("File end reached while reading")

				if len(read_buffer) == 0:
					if len(buffer) == 0:
						raise StreamEnded("Stream end reached")

					raise StreamAbruptlyEnded("File end reached while reading")

				buffer.extend(read_buffer)
			except BlockingIOError:
				time_left = None

				if timeout is not None:
					time_left = timeout - monotonic() + start_time

					if time_left <= 0:
						raise TimeoutError("Timed out")

				read_ready, _, _ = select((self._file_descriptor, ), (), (), time_left)

				if not read_ready:
					raise TimeoutError("The operation timed out")

		return bytes(buffer)

	def _write(self, buffer: bytes, timeout: float | None = None) -> None:
		from time import monotonic
		from select import select
		from os import write

		start_time = monotonic()
		count = 0

		while count < len(buffer):
			try:
				count += write(self._file_descriptor, memoryview(buffer)[count:])
			except BlockingIOError:
				time_left = None

				if timeout is not None:
					time_left = timeout - monotonic() + start_time

					if time_left <= 0:
						raise TimeoutError("Timed out")

				_, write_ready, _ = select((), (self._file_descriptor, ), (), time_left)

				if not write_ready:
					raise TimeoutError("The operation timed out")

	def recv_packet(self, header_timeout: float | None = 1.0, body_timeout: float | None = 2.0, immediate: bool = False) -> bytes:
		from struct import unpack

		try:
			try:
				packet_length = unpack("!I", self._read(4, header_timeout, immediate))[0]
			except StreamAbruptlyEnded:
				raise StreamAbruptlyEnded("The stream ended while reading the packet header")
			except StreamError:
				raise

			if packet_length > self.packet_max_size:
				raise ValueError("The packet is too big")

			try:
				return self._read(packet_length, body_timeout)
			except (IOError, EOFError):
				raise StreamAbruptlyEnded("The stream ended while reading the packet body")
		except StreamError:
			raise
		except TimeoutError:
			raise
		except IOError as exception:
			raise StreamError("An error occurred") from exception

	def send_packet(self, data: bytes, send_timeout: float | None = 2.0) -> None:
		from struct import pack

		if len(data) > self.packet_max_size:
			raise ValueError("The packet is too big")

		packet = pack("!I", len(data)) + data

		try:
			self._write(packet, send_timeout)
		except TimeoutError:
			raise
		except OSError as exception:
			raise StreamClosed("The connection was closed") from exception

	def get_file_descriptor(self) -> int:
		return self._file_descriptor

	def close(self) -> None:
		from os import close

		close(self._file_descriptor)
