from typing import TYPE_CHECKING


if TYPE_CHECKING:
	from .event_manager import JSONSerializable

class RPCManager:
	"""RPC Manager implementation"""

	if TYPE_CHECKING:
		from .communication import AbstractPacketStream
		from .event_manager import EventManager
		from .packet_manager import CloseState
		from typing import Literal, Callable
		from itertools import count
		from threading import Lock
		from queue import Queue

	_event_manager: EventManager

	_bound_procedures_lock: Lock
	_bound_procedures: dict[str, Callable[..., None]]

	_call_timeout: float

	_calls_lock: Lock
	_calls: dict[str, Queue[tuple[JSONSerializable | None, str | None]]]

	_call_counter: count

	def __init__(self, stream: AbstractPacketStream, call_timeout: float = 3.0, max_concurrent_calls: int = 4):
		"""
			Create a new RPCManager instance.

			:param stream: The stream used as transport.
			:param call_timeout: The timeout for calls.
			:param max_concurrent_calls: The number of threads the call executor will be able to use to execute calls.
		"""

		from .event_manager import EventManager
		from itertools import count
		from threading import Lock

		self._event_manager = EventManager(stream, call_timeout, max_concurrent_calls)
		self._event_manager.bind(self._return, "return")
		self._event_manager.bind(self._call, "call")

		self._call_timeout = call_timeout

		self._bound_procedures_lock = Lock()
		self._bound_procedures = {}

		self._call_counter = count()
		self._calls_lock = Lock()
		self._calls = {}


	def _call(self, name: str, call_id: str, positional_arguments, keyword_arguments):
		with self._bound_procedures_lock:
			if name not in self._bound_procedures:
				self._event_manager.call("return", call_id, error = f"Procedure {name} not bound.")

				return

			procedure = self._bound_procedures[name]

		try:
			return_value = procedure(*positional_arguments, **keyword_arguments)

			self._event_manager.call("return", call_id, return_value)
		except BaseException as exception:
			self._event_manager.call("return", call_id, error = f"Procedure {name} raised {exception.__class__.__name__}: {exception}.")

	def _return(self, call_id: str, return_value: JSONSerializable | None = None, error: str | None = None):
		with self._calls_lock:
			if call_id not in self._calls:
				return

			return_queue = self._calls[call_id]

		return_queue.put((return_value, error))


	def bind_event(self, event: Literal["close"], callback: Callable[[CloseState], None]):
		"""
			Bind a function to an event.

			:param event: The event to bind to.
			:param callback: The function to bind.
		"""

		self._event_manager.bind_event(event, callback)

	def bind(self, procedure: Callable[..., JSONSerializable], name: str | None = None):
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
			Queue a remote procedure call and wait for response.

			:param name: The name of the procedure.
			:param positional_arguments: Positional arguments, passed to the procedure.
			:param keyword_arguments: Keyword arguments, passed to the procedure.

			:raises ManagerClosed: If the manager is closed at the time of queueing.
			:raises RuntimeError: If an error occurred while running the remote procedure.
		"""

		from queue import Queue, Empty

		call_id: str = str(next(self._call_counter))

		return_queue = Queue(1)

		with self._calls_lock:
			self._calls[call_id] = return_queue

		self._event_manager.call("call", name, call_id, positional_arguments, keyword_arguments)

		try:
			return_value, error = return_queue.get(timeout = self._call_timeout)

			if error:
				raise RuntimeError(error)

			return return_value
		except Empty:
			raise TimeoutError("The call timed out") from None
		finally:
			with self._calls_lock:
				if call_id in self._calls:
					del self._calls[call_id]

	def close(self, wait_futures: bool = True):
		"""
			Close the RPCManager and optionally wait for calls to finish.

			:param wait_futures: If set to true, the event manager will wait for currently executing calls to finish before closing.
		"""

		self._event_manager.close(wait_futures)

	def join(self):
		"""
			Wait for stream close and return state.

			:return CloseState: The state of the stream worker thread at close.
		"""

		return self._event_manager.join()
