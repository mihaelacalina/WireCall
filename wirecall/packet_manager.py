from typing import TYPE_CHECKING


class PacketManager:
	if TYPE_CHECKING:
		from .communication import AbstractPacketStream
		from typing import Callable

	_stream: AbstractPacketStream
	_send_timeout: float
	_recv_timeout: float

	_events: dict[str, list[Callable]]

	def __init__(self, stream: AbstractPacketStream, recv_timeout: float = 4.0, send_timeout: float = 4.0):
		from threading import Thread

		self._stream = stream
		self._recv_timeout = recv_timeout
		self._send_timeout = send_timeout

		self._thread_object = Thread(
			target = self._worker,
			name = "Packet Manager Thread"
		)

		self._thread_object.start()

	def _worker(self):
		while True:
			pass