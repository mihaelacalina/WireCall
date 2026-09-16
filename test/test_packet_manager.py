from threading import Event


def create_manager_pair():
	from wirecall.communication import TCPSocketPacketStream
	from wirecall.packet_manager import PacketManager
	from socket import socketpair

	sock_0, sock_1 = socketpair()

	return PacketManager(TCPSocketPacketStream(sock_0)), PacketManager(TCPSocketPacketStream(sock_1))


send_received = False

def send_worker(_: bytes):
	global send_received

	send_received = True

def test_send():
	man_0, man_1 = create_manager_pair()

	man_1.bind_event("packet", send_worker)

	man_0.send_packet(b"awa")

	man_0.close()
	man_0.join()

	assert send_received


throughput_recv_counter = 0
throughput_done = Event()

def throughput_helper(_: bytes):
	global throughput_recv_counter

	throughput_recv_counter += 1

def test_throughput():
	from time import monotonic

	man_0, man_1 = create_manager_pair()

	man_1.bind_event("packet", throughput_helper)

	start_time = monotonic()

	for i in range(1000):
		man_0.send_packet(b"awa")

	man_0.close()
	man_1.join()

	print(f"Throughput: {throughput_recv_counter / (monotonic() - start_time)}", end = " ")
