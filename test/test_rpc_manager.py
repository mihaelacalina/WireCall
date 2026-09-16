from threading import Event
from pytest import raises

def create_manager_pair():
	from wirecall.communication import TCPSocketPacketStream
	from wirecall.rpc_manager import RPCManager
	from socket import socketpair

	sock_0, sock_1 = socketpair()

	return RPCManager(TCPSocketPacketStream(sock_0)), RPCManager(TCPSocketPacketStream(sock_1))


send_received = False

def send_helper():
	global send_received

	send_received = True

def test_send():
	man_0, man_1 = create_manager_pair()

	man_1.bind(send_helper)

	man_0.call("send_helper")

	man_0.close()
	man_0.join()

	assert send_received


def test_return():
	man_0, man_1 = create_manager_pair()

	man_1.bind(lambda : 674523, "test")

	assert man_0.call("test") == 674523

	man_0.close()
	man_0.join()

def test_non_existent():
	man_0, man_1 = create_manager_pair()

	with raises(RuntimeError):
		man_0.call("test")

	man_0.close()
	man_0.join()


throughput_recv_counter = 0
throughput_done = Event()

def throughput_helper():
	global throughput_recv_counter

	throughput_recv_counter += 1

def test_throughput():
	from time import monotonic

	man_0, man_1 = create_manager_pair()

	man_1.bind(throughput_helper)

	start_time = monotonic()

	for i in range(1000):
		man_0.call("throughput_helper")

	man_0.close()
	man_1.join()

	print(f"Throughput: {throughput_recv_counter / (monotonic() - start_time)}", end = " ")
