import struct
import socket

from .logger import logging
from .constants import MINIMUM_ACK_PDU_LEN, PduType, MINIMUM_ACK_INFO_ENTRYLEN, Option
from .utils import int_from_bytes, int_to_bytes


## AckInfoEntry class definition
#
# This field contains the Source_ID of the transmitting node, the Message_ID
# and a variable length list of the sequence numbers of the Data_PDUs that
# have not yet been received.
#
# 0                   1                   2                   3
# 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
# One ACK_Info_Entry            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
#                               |    Length_of_ACK_Info_Entry   |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |          Reserved           |  Highest_Seq_Number_in_Window |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                         Source_ID                           |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                     Message_ID (MSID)                       |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# | Count_Of_Missing_Seq_Number | Missing_Data_PDU_Seq_Number 1 |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |...                          | Missing_Data_PDU_Seq_Number n |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                                                             |
# +                           TValue                            +
# |                                                             |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                                                             |
# |                  Options (variable length)                  |
# |                                                             |
# +-------------------------------------------------------------+
#
class AckInfoEntry:
    def __init__(self):
        self.seqnohi = 0  # Highest Sequence Number which was received from the sender
        self.remote_ipaddr = (
            ""  # Identifier of the transmitting node from the AddressPdu
        )
        self.msid = 0  # Message Identifier received from AddressPdu
        self.missing_seqnos = []  # List of missing sequence numbers
        # The Tval field of 8 bytes may be used for reporting the time elapsed
        # (in units of 100ms) from the receiver received the Address_PDU to the
        # receiver sent back the ACK_PDU. The sender may use this time to estimate
        # the time it has taken to transmit the Data_PDUs and to adaptively adjust
        # the parameter “PDU_DELAY” used for congestion control.
        # No synchronization of clocks is required.
        self.tvalue = 0  # Time elapsed (in units of 100ms)
        self.tsecr = 0  # Timestamp echo reply

    def length(self):
        return MINIMUM_ACK_INFO_ENTRYLEN + 2 * len(self.missing_seqnos) + 12

    def to_buffer(self):
        srcid = int_from_bytes(socket.inet_aton(self.remote_ipaddr))
        packet = bytearray()
        packet.extend(
            struct.pack(
                "<HHHIIH",
                self.length(),  # 0: lengthOfPDU
                0,  # 1: Reserved
                self.seqnohi,  # 2: Highest Sequence Number in window
                srcid,  # 3: Source Id
                self.msid,  # 4: Message identifier
                len(self.missing_seqnos),  # 5: Count of missing sequence number
            )
        )
        for i, val in enumerate(self.missing_seqnos):
            packet.extend(struct.pack("<H", val))
        packet.extend(struct.pack("<Q", self.tvalue))
        packet.extend(
            struct.pack("<ccHQ", bytes([int(Option.TsEcr)]), bytes([12]), 0, self.tsecr)
        )
        return packet

    def from_buffer(self, buffer):
        if len(buffer) < MINIMUM_ACK_INFO_ENTRYLEN:
            logging.error(
                "RX: AckInfoEntry.from_buffer() FAILED with: Message to small"
            )
            return 0
        # Read AckInfoEntry Header
        unpacked_data = struct.unpack("<HHHIIH", buffer[:16])
        length = unpacked_data[0]
        reserved = unpacked_data[1]
        self.seqnohi = unpacked_data[2]
        self.remote_ipaddr = socket.inet_ntoa(int_to_bytes(unpacked_data[3]))
        self.msid = unpacked_data[4]
        total_entries = unpacked_data[5]
        if length > len(buffer):
            logging.error("RX AckInfoEntry.from_buffer() FAILED with: Buffer too small")
            return 0
        if reserved != 0:
            logging.error(
                "RX AckInfoEntry.from_buffer() FAILED with: Reserved field is not zero"
            )
            return 0
        if length != MINIMUM_ACK_INFO_ENTRYLEN + 12 + total_entries * 2:
            logging.error("RX AckInfoEntry.from_buffer() FAILED with: Corrupt PDU")
            return 0
        num_entries = 0
        nbytes = 16
        while num_entries < total_entries:
            self.missing_seqnos.append(
                (struct.unpack("<H", buffer[nbytes : nbytes + 2]))[0]
            )
            nbytes = nbytes + 2
            num_entries = num_entries + 1
        self.tvalue = struct.unpack("<Q", buffer[nbytes : nbytes + 8])[0]
        nbytes = nbytes + 8

        # Read additional options
        while nbytes + 2 < length:
            type = buffer[nbytes]
            optlen = buffer[nbytes + 1]
            if optlen == 0:
                logging.error(
                    "RX: AckInfoEntry.from_buffer() FAILED with: Invalid option TLV"
                )
                return 0
            if type == int(Option.TsEcr):
                if optlen != 12:
                    logging.error(
                        "RX: AckInfoEntry.from_buffer() FAILED with: Invalid option len of tsEcr"
                    )
                    return 0
                tlv_unpacked = struct.unpack("<ccHQ", buffer[nbytes : nbytes + 12])
                self.tsecr = tlv_unpacked[3]
            else:
                logging.info("RX: Ignore unkown option {}".format(type))
            nbytes = nbytes + optlen
        return nbytes


## AckPDU class definition
#
# This PDU is generated by a receiving node identified by the Source_ID_of_ACK_Sender
# and is used to inform the transmitting node of the status of one or more messages
# received. This information is composed as one or more entries of the list of
# ACK_Info_Entries. Each of these entries holds a message identifier (Source_ID
# and Message_ID) and a list of Missing_Data_PDU_Seq_Numbers, which may contain
# a list of those Data_PDUs not yet received. If this list is empty, the message
# identified by Source ID and Message ID has been correctly received.
#
# 0                   1                   2                   3
# 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |        Length_of_PDU        |    Priority   |   |  PDU_Type |  4
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |           unused            |            Checksum           |  8
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                  Source_ID_of_ACK_Sender                    | 12
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |  Count_of_ACK_Info_Entries  |                               | 14
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
# |                                                             |
# |          List of ACK_Info_Entries (variable length)         |
# |                                                             |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
#


class AckPdu:
    def __init__(self):
        self.src_ipaddr = ""  # IP address of ACK emitter
        self.info_entries = []  # List of AckInfo entries

    def length(self):
        length = MINIMUM_ACK_PDU_LEN
        for i, val in enumerate(self.info_entries):
            length += val.length()
        return length

    def to_buffer(self):
        srcid = int_from_bytes(socket.inet_aton(self.src_ipaddr))
        packet = bytearray()
        packet.extend(
            struct.pack(
                "<HccHHIH",
                self.length(),  # 0: lengthOfPDU
                bytes([0]),  # 1: priority
                bytes([int(PduType.Ack)]),  # 2: PDU type
                0,  # 3: unused
                0,  # 4: checksum
                srcid,  # 5: source-id
                len(self.info_entries),  # 6: Count of AckInfo entries
            )
        )
        for i, val in enumerate(self.info_entries):
            packet.extend(val.to_buffer())
        return packet

    def from_buffer(self, buffer):
        if len(buffer) < MINIMUM_ACK_PDU_LEN:
            logging.error("RX: AckPdu.from_buffer() FAILED with: Message to small")
            return 0

        # Read AddressPdu Header
        unpacked_data = struct.unpack("<HccHHIH", buffer[:MINIMUM_ACK_PDU_LEN])
        length = unpacked_data[0]
        unused = unpacked_data[3]
        # chksum = unpacked_data[4]
        self.src_ipaddr = self.src_ipaddr = socket.inet_ntoa(
            int_to_bytes(unpacked_data[5])
        )
        total_entries = unpacked_data[6]
        if len(buffer) < length:
            logging.error("RX: AckPdu.from_buffer() FAILED with: Message to small")
            return 0
        if unused != 0:
            logging.error(
                "RX: AckPdu.from_buffer() FAILED witg: Unused field is not zero"
            )
            return 0

        # Read AckInfo entries
        num_entries = 0
        nbytes = MINIMUM_ACK_PDU_LEN
        while num_entries < total_entries:
            infolen = struct.unpack("<H", buffer[nbytes : nbytes + 2])[0]
            if nbytes + infolen > len(buffer):
                logging.error(
                    "RX: AckPdu.from_buffer() FAILED with: Corrupt AckInfoEntry"
                )
                return 0
            ack_info_entry = AckInfoEntry()
            consumed = ack_info_entry.from_buffer(buffer[nbytes:])
            if consumed < 0:
                logging.error(
                    "RX: AckPdu.from_buffer() FAILED with: Invalid AckInfoEntry"
                )
                return 0
            self.info_entries.append(ack_info_entry)
            nbytes = nbytes + infolen
            num_entries = num_entries + 1
        return nbytes

    def log(self, rxtx):
        logging.debug(
            "{} *** AckPdu *************************************************".format(
                rxtx
            )
        )
        logging.debug("{} * src:{}".format(rxtx, self.src_ipaddr))
        for i, val in enumerate(self.info_entries):
            logging.debug(
                "{} * seqnohi:{} remote_ipaddr:{} msid:{} missed:{} tval:{} tsecr:{}".format(
                    rxtx,
                    val.seqnohi,
                    val.remote_ipaddr,
                    val.msid,
                    val.missing_seqnos,
                    val.tvalue,
                    val.tsecr,
                )
            )
        logging.debug(
            "{} ****************************************************************".format(
                rxtx
            )
        )
