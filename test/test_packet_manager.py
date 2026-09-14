from wirecall.communication import UDPSocketPacketStream
from pytest import raises


def create_stream_pair():
	from socket import socketpair, SOCK_DGRAM

	sock_0, sock_1 = socketpair(type = SOCK_DGRAM)

	return UDPSocketPacketStream(sock_0), UDPSocketPacketStream(sock_1)


def test_io():
	from random import randbytes

	packet_a = randbytes(32)
	packet_b = randbytes(32)

	stream_a, stream_b = create_stream_pair()

	stream_a.send_packet(packet_a)
	stream_b.send_packet(packet_b)

	assert stream_a.recv_packet() == packet_b
	assert stream_b.recv_packet() == packet_a

	stream_a.close()
	stream_b.close()

def test_close_send():
	from wirecall.communication import StreamClosed
	from random import randbytes

	packet_a = randbytes(32)

	stream_a, stream_b = create_stream_pair()

	stream_b.close()

	with raises(StreamClosed):
		stream_a.send_packet(packet_a)
		stream_a.send_packet(packet_a)
		stream_a.send_packet(packet_a)

	stream_a.close()

def test_close_receive():
	from wirecall.communication import StreamEnded

	stream_a, stream_b = create_stream_pair()

	stream_b.close()

	with raises(StreamEnded):
		stream_a.recv_packet()

	stream_a.close()

def test_close_receive_immediate():
	from wirecall.communication import StreamEnded

	stream_a, stream_b = create_stream_pair()

	stream_b.close()

	with raises(StreamEnded):
		stream_a.recv_packet(immediate = True)

	stream_a.close()

def test_immediate():
	from wirecall.communication import PacketUnavailable
	from time import sleep

	stream_a, stream_b = create_stream_pair()

	stream_b.send_packet(b"test0")
	sleep(0.05)
	stream_a.recv_packet(immediate = True)

	with raises(PacketUnavailable):
		stream_a.recv_packet(immediate = True)

	stream_a.close()
	stream_b.close()

def test_read_timeout():
	from time import monotonic

	stream_a, stream_b = create_stream_pair()

	start = monotonic()

	with raises(TimeoutError):
		stream_a.recv_packet(header_timeout = 0.1)

	delta = monotonic() - start

	assert delta > 0.09
	assert delta < 0.11

	stream_a.close()
	stream_b.close()

def test_send_timeout():
	from socket import SOCK_DGRAM, SOL_SOCKET, SO_RCVBUF, SO_SNDBUF
	from socket import socketpair
	from random import randbytes
	from time import monotonic

	sock_a, sock_b = socketpair(type = SOCK_DGRAM)

	sock_a.setsockopt(SOL_SOCKET, SO_SNDBUF, 4096)
	sock_b.setsockopt(SOL_SOCKET, SO_RCVBUF, 4096)

	stream_a = UDPSocketPacketStream(sock_a)
	big_chunk = randbytes(UDPSocketPacketStream.packet_max_size)

	start = monotonic()

	with raises(TimeoutError):
		for _ in range(100):
			stream_a.send_packet(big_chunk, send_timeout = 0.1)

	delta = monotonic() - start

	assert delta > 0.09
	assert delta < 0.11

	stream_a.close()
	sock_b.close()

throughput_recv_counter = 0

def throughput_worker(stream, event):
	from wirecall.communication import StreamEnded

	global throughput_recv_counter

	try:
		while True:
			stream.recv_packet(0.5)

			throughput_recv_counter += 1
	except (StreamEnded, TimeoutError):
		pass

	event.set()

def test_throughput():
	from threading import Thread, Event
	from time import monotonic

	global throughput_recv_counter

	throughput_recv_counter = 0

	data = b"A" * 1024

	sender, receiver = create_stream_pair()
	finished = Event()

	Thread(
		target = throughput_worker,
		args = (receiver, finished),
		daemon = True
	).start()

	start_time = monotonic()

	for _ in range(1000):
		sender.send_packet(data)

	sender.close()
	finished.wait()
	receiver.close()

	throughput = throughput_recv_counter / (monotonic() - start_time)

	assert throughput_recv_counter == 1000
	assert throughput > 500

	print(f"Throughput of {throughput}", end = " ")

def test_throughput_max():
	from threading import Thread, Event
	from time import monotonic

	global throughput_recv_counter

	throughput_recv_counter = 0

	data = b"A" * UDPSocketPacketStream.packet_max_size

	sender, receiver = create_stream_pair()
	finished = Event()

	Thread(
		target = throughput_worker,
		args = (receiver, finished),
		daemon = True
	).start()

	start_time = monotonic()

	for _ in range(1000):
		sender.send_packet(data)

	sender.close()
	finished.wait()
	receiver.close()

	throughput = throughput_recv_counter / (monotonic() - start_time)

	assert throughput_recv_counter == 1000
	assert throughput > 500

	print(f"Throughput of {throughput}", end = " ")