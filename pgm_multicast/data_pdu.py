import struct
import socket

from .logger import logging
from .constants import PduType, DATA_PDU_HDRLEN
from .utils import int_to_bytes, int_from_bytes


# 0                   1                   2                   3
# 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |        Length_of_PDU        |    Priority   |   |  PDU_Type | 4
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |   Number_of_PDUs_in_Window  |  Highest_Seq_Number_in_Window | 8
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |    Sequence_Number_of_PDU   |            Checksum           | 12
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |  Sequence_Number_of_Window  |            Reserved           | 16
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                        Source_ID                            | 20
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                     Message_ID (MSID)                       | 24
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                                                             |
# |             Fragment of Data (variable length)              |
# |                                                             |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
#
class DataPdu:
    def __init__(self):
        self.cwnd = 0  # Number of PDUs in current transmission window
        self.seqnohi = 0  # Highest sequence number in current transmission window
        self.cwnd_seqno = 0  # Sequence number of transmission window
        self.seqno = 0  # Sequence number of PDU within the original message
        self.src_ipaddr = ""  # IPv4 address of sender
        self.msid = 0  # MSID is a unique identifier created within the scope of Source_ID by the transmitter. */
        self.data = bytearray()  # Buffer holding the data

    def len(self):
        return DATA_PDU_HDRLEN + len(self.data)

    def log(self, rxtx):
        logging.debug(
            "{} *** Data *************************************************".format(rxtx)
        )
        logging.debug(
            "{} * cwnd:{} seqnohi:{} cwnd_seqno:{} seqno:{} srcid:{} msid:{}".format(
                rxtx,
                self.cwnd,
                self.seqnohi,
                self.cwnd_seqno,
                self.seqno,
                self.src_ipaddr,
                self.msid,
            )
        )
        logging.debug(
            "{} ****************************************************************".format(
                rxtx
            )
        )

    def to_buffer(self):
        srcid = int_from_bytes(socket.inet_aton(self.src_ipaddr))
        packet = bytearray()
        packet.extend(
            struct.pack(
                "<HccHHHHHHII",
                self.len(),  # 0: lengthOfPDU
                bytes([0]),  # 1: priority
                bytes([int(PduType.Data)]),  # 2: PDU Type
                self.cwnd,  # 3: Number of PDUs in window
                self.seqnohi,  # 4: Highest number in window
                self.seqno,  # 5: Sequence number of PDU
                0,  # 6: Checksum
                self.cwnd_seqno,  # 7: Sequence number of window
                0,  # 8: Reserved
                srcid,  # 9: Source Identifier
                self.msid,  # 10: Message-ID
            )
        )
        packet.extend(self.data)
        return packet

    def from_buffer(self, buffer):
        if len(buffer) < DATA_PDU_HDRLEN:
            logging.error("RX: DataPdu.from_buffer() FAILED with: Message to small")
            return 0
        # Read DataPdu Header
        unpacked_data = struct.unpack("<HccHHHHHHII", buffer[:DATA_PDU_HDRLEN])
        length = unpacked_data[0]
        self.cwnd = unpacked_data[3]
        self.seqnohi = unpacked_data[4]
        self.seqno = unpacked_data[5]
        self.cwnd_seqno = unpacked_data[7]
        reserved = unpacked_data[8]
        self.src_ipaddr = socket.inet_ntoa(int_to_bytes(unpacked_data[9]))
        self.msid = unpacked_data[10]
        if length < DATA_PDU_HDRLEN:
            logging.error("RX: DataPdu.from_buffer() FAILED with: Invalid length field")
            return 0
        if length > len(buffer):
            logging.error("RX: DataPdu.from_buffer() FAILED with: Message to small")
            return 0
        if reserved != 0:
            logging.error(
                "RX: DataPdu.from_buffer() FAILED with: Reserved field is not zero"
            )
            return 0
        self.data.extend(buffer[DATA_PDU_HDRLEN:length])
        return length
