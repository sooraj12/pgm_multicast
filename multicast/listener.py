import socket
import struct
import requests
import threading
import gzip
import msgpack
import reedsolo
import itertools
from io import BytesIO

from .constants import (
    FAMILY,
    INTERFACE_IP,
    PORT,
    GROUP_IP,
    iface_index,
    BUFFER_SIZE,
    BUFFER_OFFSET,
    HEADER,
)
from .helpers import get_data_from_packet, get_header_from_packet, unpack_header
from .utils import is_windows, make_ip_mreqn_struct

shutdown_event = threading.Event()

listener = socket.socket(family=FAMILY, type=socket.SOCK_DGRAM)


def listen():
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    if is_windows:
        listener.bind((INTERFACE_IP, PORT))
        mreq_bytes = struct.pack("!4sI", socket.inet_aton(GROUP_IP), iface_index)
        listener.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq_bytes)
        listener.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, False)
    else:
        listener.bind((GROUP_IP, PORT))
        listener.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            make_ip_mreqn_struct(GROUP_IP, iface_index, INTERFACE_IP),
        )

    rs = reedsolo.RSCodec(4)

    print("listening for messages")

    decode_directory = {}
    while not shutdown_event.is_set():
        try:
            packet, _ = listener.recvfrom(BUFFER_SIZE + BUFFER_OFFSET, 0)
            if packet is not None:
                header = get_header_from_packet(packet)
                data = get_data_from_packet(packet)
                # unpacking the header to get sequence number, unique identifier
                # total number of chunks and the header identifier
                index, size, uid, header_id = unpack_header(header)

                if header_id == HEADER:
                    decoded = rs.decode(data)[0]
                    if uid not in decode_directory:
                        decode_directory[uid] = []
                    decode_directory[uid].append((index, decoded))
                    if len(decode_directory[uid]) == size:
                        chunks = decode_directory[uid]
                        chunks.sort(key=lambda x: x[0])
                        data_array = [x[1] for x in chunks]
                        byte_iterable = itertools.chain.from_iterable(data_array)
                        byte_object = bytes(byte_iterable)
                        buffer = BytesIO(byte_object)
                        with gzip.GzipFile(fileobj=buffer) as f:
                            decompressed_content = f.read()
                            message = msgpack.unpackb(decompressed_content, raw=False)
                        del decode_directory[uid]
                        print(
                            f"=================Buffer:{decode_directory}============="
                        )
                        try:
                            dest_ips = message.get("dest_ips", [])
                            if INTERFACE_IP in dest_ips or len(dest_ips) == 0:
                                print(f"==============={message}==================")
                                url = "http://localhost:5000/webhook"
                                requests.post(url, json=message)
                        except Exception as e:
                            print(e)

        except Exception as e:
            print(e)
            continue


def shutdown():
    if listener:
        listener.close()
    shutdown_event.set()
