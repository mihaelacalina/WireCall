from wirecall.communication import FilePacketStream
from pytest import raises


def create_stream_pair():
	from os import pipe

	fd_0, fd_1 = pipe()

	return FilePacketStream(fd_1), FilePacketStream(fd_0)


def test_io():
	from random import randbytes

	packet_a = randbytes(32)

	stream_a, stream_b = create_stream_pair()

	stream_a.send_packet(packet_a)

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
	from random import randbytes
	from time import monotonic

	stream_a, stream_b = create_stream_pair()

	big_chunk = randbytes(1024 * 1024)

	start = monotonic()

	with raises(TimeoutError):
		for i in range(100):
			stream_a.send_packet(big_chunk, send_timeout = 0.1)

	delta = monotonic() - start

	assert delta > 0.09
	assert delta < 0.11

	stream_a.close()
	stream_b.close()

throughput_recv_counter = 0

def throughput_worker(stream, event):
	from wirecall.communication import StreamEnded

	global throughput_recv_counter

	try:
		while True:
			stream.recv_packet(None)

			throughput_recv_counter += 1
	except StreamEnded:
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

	print(f"Throughput of {throughput}", end = " ")

def test_throughput_max():
	from threading import Thread, Event
	from time import monotonic

	global throughput_recv_counter

	throughput_recv_counter = 0

	data = b"A" * FilePacketStream.packet_max_size

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

	print(f"Throughput of {throughput}", end = " ")