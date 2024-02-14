from .logger import logging
from .constants import PduType, MINIMUM_PACKET_LEN

import struct


################################################################################
## Pdu Header
#
# 0                   1                   2                   3
# 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |        Length_of_PDU        |    Priority   |MAP|  PDU_Type |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
#
class Pdu:
    def __init__(self):
        self.len = 0  # The length of the PDU in bytes
        self.priority = 0  # The priority of the message transfer
        self.map = 0  # The map field is only filled at ACK PDUs
        self.type = int(PduType.Unkown)  # The type of the PDU

    def from_buffer(self, buffer):
        mask = bytes([0x3F])
        if len(buffer) < MINIMUM_PACKET_LEN:
            logging.error("Pdu.from_buffer() FAILED with: Message too small")
            return 0
        unpacked_data = struct.unpack("<Hcc", buffer[:MINIMUM_PACKET_LEN])
        self.len = unpacked_data[0]
        self.priority = int.from_bytes(unpacked_data[1], byteorder="big")
        self.type = int.from_bytes(
            bytes([unpacked_data[2][0] & mask[0]]), byteorder="big"
        )
        return MINIMUM_PACKET_LEN

    def log(self, rxtx):
        logging.debug(
            "{}: --- PDU --------------------------------------------------------".format(
                rxtx
            )
        )
        logging.debug(
            "{}: - Len[{}] Prio[{}] Type[{}]".format(
                rxtx, self.len, self.priority, self.type
            )
        )
        logging.debug(
            "{}: ----------------------------------------------------------------".format(
                rxtx
            )
        )
