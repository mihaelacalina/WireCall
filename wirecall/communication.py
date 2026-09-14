from abc import ABC, abstractmethod
from typing import TYPE_CHECKING


class StreamError(IOError):
	pass

class StreamExhaustedError(StreamError):
	"""Raised when the stream end is reached under illegal conditions."""

class StreamEnded(StreamError):
	"""Raised when the stream end is reached under normal conditions."""

class StreamClosedError(StreamError):
	"""Raised when the stream cannot be written to as a result of it being closed."""

class PacketUnavailable(StreamError):
	"""Raised when no bytes are immediately available to be read."""


class AbstractPacketStream(ABC):
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

			:return bytes: The packet contents.
		"""

		pass

	@abstractmethod
	def send_packet(self, data: bytes, timeout: float | None = 2.0) -> None:
		"""
			Send a packet down the stream.

			This operation may block.

			:param data: The packet contents.
			:param timeout: The timeout for each write operation.

			:raises StreamClosedError: If any of the write operations fail.
			:raises TimeoutError: If the packet could not be written in time.
		"""

		pass

	@abstractmethod
	def close(self) -> None:
		"""
			Closes the underlying file descriptor.
		"""

		pass

class SocketPacketStream(AbstractPacketStream):
	if TYPE_CHECKING:
		from socket import socket as Socket

	_is_ssl_socket: bool
	_socket: Socket

	def __init__(self, socket: Socket, is_ssl_socket: bool = False):
		self._is_ssl_socket = is_ssl_socket
		self._socket = socket

	@abstractmethod
	def recv_packet(self, header_timeout: float | None = 1.0, content_timeout: float | None = 2.0, immediate: bool = False) -> bytes:
		from socket import MSG_PEEK, MSG_DONTWAIT
		from struct import unpack
		from time import monotonic

		# region Immediate buffer check

		if immediate:
			if self._is_ssl_socket:
				if self._socket.pending() <= 0:
					raise PacketUnavailable()
			else:
				try:
					self._socket.recv(1, MSG_PEEK | MSG_DONTWAIT)
				except BlockingIOError:
					raise PacketUnavailable()
				except BaseException as exception:
					raise StreamError("An error occurred") from exception

		# endregion

		# region Header reading

		initial_time = monotonic()
		header_buffer = bytearray()

		while len(header_buffer) < 4:
			if header_timeout is not None:
				self._socket.settimeout(header_timeout - monotonic() - initial_time)

			try:
				read_buffer = self._socket.recv(4 - len(header_buffer))
			except BaseException as exception:
				raise StreamError("An error occurred") from exception

			if len(read_buffer) == 0:
				raise StreamExhaustedError("The stream ended while reading the packet header.")

			header_buffer.extend(read_buffer)

		# endregion

		packet_length = unpack("!I", bytes(header_buffer))[0]

		initial_time = monotonic()
		packet_buffer = bytearray()

		while len(packet_buffer) < packet_length:
			if content_timeout is not None:
				self._socket.settimeout(content_timeout - monotonic() - initial_time)

			try:
				read_buffer = self._socket.recv(packet_length - len(packet_buffer))
			except BaseException as exception:
				raise StreamError("An error occurred") from exception

			if len(read_buffer) == 0:
				raise StreamExhaustedError("The stream ended while reading the packet body.")

			packet_buffer.extend(read_buffer)

		return packet_buffer

	@abstractmethod
	def send_packet(self, data: bytes, send_timeout: float | None = 2.0) -> None:
		from struct import pack

		packet = bytearray()

		packet.extend(pack("!I", len(data)))
		packet.extend(data)

		try:
			self._socket.settimeout(send_timeout)
			self._socket.sendall(bytes(packet))
		except OSError as exception:
			raise StreamClosedError("The connection was closed") from exception

	@abstractmethod
	def close(self) -> None:
		self._socket.close()

	@classmethod
	def create_pair(cls):
		from socket import socketpair

		sock_0, sock_1 = socketpair()

		return cls(sock_0), cls(sock_1)
