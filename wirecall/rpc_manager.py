from typing import TYPE_CHECKING

if TYPE_CHECKING:
	type JSONSerializable = bool | int | float | str | list[JSONSerializable] | dict[str, JSONSerializable]

class EventManager:
	if TYPE_CHECKING:
		from concurrent.futures.thread import ThreadPoolExecutor
		from .communication import AbstractPacketStream
		from .packet_manager import PacketManager
		from typing import Literal, Callable
		from threading import Lock

	_packet_manager: PacketManager

	_executor: ThreadPoolExecutor

	_bound_procedures_lock: Lock
	_bound_procedures: dict[str, Callable[..., None]]

	def __init__(self, stream: AbstractPacketStream, call_timeout: float = 3.0, max_concurrent_calls: int = 4):
		from concurrent.futures.thread import ThreadPoolExecutor
		from .packet_manager import PacketManager
		from threading import Lock

		self._packet_manager = PacketManager(stream, call_timeout, call_timeout)
		self._packet_manager.bind("packet", self._on_packet)

		self._executor = ThreadPoolExecutor(max_concurrent_calls)

		self._bound_procedures_lock = Lock()
		self._bound_procedures = {}

	def bind_event(self, event: Literal["close"], callback: Callable[[], None]):
		self._packet_manager.bind(event, callback)

	def bind(self, procedure: Callable[..., None], name: str | None = None):
		if name is None:
			name = procedure.__name__

		with self._bound_procedures_lock:
			self._bound_procedures[name] = procedure

		return procedure

	def call(self, name: str, *positional_arguments, **keyword_arguments):
		from json import dumps

		data = {
			"procedure_name": name,
			"positional_arguments": positional_arguments,
			"keyword_arguments" : keyword_arguments
		}

		self._packet_manager.send_packet(dumps(data).encode())

	def _call(self, procedure: Callable, procedure_name: str, positional_arguments, keyword_arguments):
		from warnings import warn

		try:
			self._executor.submit(procedure, *positional_arguments, **keyword_arguments)
		except BaseException as exception:
			warn(f"Received call for procedure \"{procedure_name}\" failed with {exception.__class__.__name__}: {exception}")

	def close(self):
		self._packet_manager.close()

	def join(self):
		return self._packet_manager.join()

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

		self._executor.submit(self._call, procedure, call["procedure_name"], call["positional_arguments"], call["keyword_arguments"])