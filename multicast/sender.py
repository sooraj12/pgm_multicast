import socket
import gzip
import msgpack
import more_itertools
import reedsolo
import time
from uuid import uuid4

from .constants import (
    FAMILY,
    INTERFACE_IP,
    iface_index,
    GROUP_IP,
    PORT,
    BUFFER_SIZE,
    RATE_LIMIT,
    SLEEP_TIME,
)
from .helpers import pack_header
from .utils import set_multicast_if


def send_message(data):
    sender = socket.socket(family=FAMILY, type=socket.SOCK_DGRAM)
    sender.bind((INTERFACE_IP, 0))  # bind any available port

    set_multicast_if(sender, iface_index, GROUP_IP, INTERFACE_IP)
    sender.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 32)
    sender.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, False)

    print(f"============={data}============")

    id = uuid4()

    # serialize
    serialized = msgpack.packb(data)
    # compress
    compressed_data = gzip.compress(serialized)
    # chunking
    chunks = list(more_itertools.chunked(compressed_data, BUFFER_SIZE))
    # encoding
    rs = reedsolo.RSCodec(4)
    counter = 0
    for i, packet in enumerate(chunks):
        if counter > RATE_LIMIT:
            print("==============Sleep==============")
            time.sleep(SLEEP_TIME)
            counter = 0
        counter += 1
        encoded = rs.encode(packet)
        # add header info
        # total chunks,
        # uniq identifier for the data, all the chunks belonging to the same data
        # will have the same identifier
        header = pack_header(i, len(chunks), id)
        packet_with_header = header + bytes(encoded)
        sender.sendto(packet_with_header, (GROUP_IP, PORT))
        print("message sent")

    sender.close()
