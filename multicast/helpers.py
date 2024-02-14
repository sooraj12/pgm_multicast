import struct
from uuid import UUID
from .constants import HEADER


def get_header_format(id_lenght):
    return ">II" + "B" * id_lenght + "BBBB"


def pack_header(sq, length, id):
    return struct.pack(get_header_format(len(id.bytes)), sq, length, *id.bytes, *HEADER)


def unpack_header(header):
    index, size, *rest = struct.unpack(">II" + "B" * 16 + "BBBB", header)
    uid = UUID(bytes=bytes(rest[0:16]))
    header_id = bytes(rest[16:])

    return index, size, uid, header_id


def get_header_from_packet(packet):
    return packet[:28]


def get_data_from_packet(packet):
    return packet[28:]
