import struct
import socket

from .constants import (
    PduType,
    Option,
    MINIMUM_ADDRESS_PDU_LEN,
    DESTINATION_ENTRY_LEN,
    OPT_TS_VAL_LEN,
)
from .logger import logging
from .utils import int_from_bytes, int_to_bytes


# 0                   1                   2                   3
# 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                      Destination ID                         |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                  Message_Sequence_Number                    |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |              Reserved_Field (variable length)               |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
#
class DestinationEntry:
    def __init__(self):
        # This field holds a unique identifier identifying a receiving node on the
        # actual multicast network (e.g. the Internet version 4 address of the
        # receiving node). Destination_ID is a unique identifier within the scope
        # of the nodes supporting P_MUL.
        self.dest_ipaddr = ""

        # This entry holds a message sequence number, which is unique for the
        # sender/receiver pair denoted by Source_ID and Destination_ID.
        # This sequence number is generated by the transmitter consecutively
        # with no omissions and is used by receivers to detect message loss.
        self.seqno = 0

    def len(self):
        return DESTINATION_ENTRY_LEN

    def to_buffer(self):
        destid = int_from_bytes(socket.inet_aton(self.dest_ipaddr))
        return struct.pack("<II", destid, self.seqno)

    def from_buffer(self, buffer):
        if len(buffer) < DESTINATION_ENTRY_LEN:
            logging.error(
                "RX: DestinationEntry.from_buffer() FAILED with: Message to small"
            )
            return 0
        unpacked_data = struct.unpack("<II", buffer[:DESTINATION_ENTRY_LEN])
        self.dest_ipaddr = socket.inet_ntoa(int_to_bytes(unpacked_data[0]))
        self.seqno = unpacked_data[1]
        return DESTINATION_ENTRY_LEN


# 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |        Length_of_PDU        |    Priority   |MAP|  PDU_Type | 4
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |     Total_Number_of_PDUs    |            Checksum           | 8
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |   Number_of_PDUs_in_Window  |  Highest_Seq_Number_in_Window | 12
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |        Data Offset          |            Reserved           | 16
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                         Source_ID                           | 20
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                      Message_ID (MSID)                      | 24
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                        Expiry_Time                          | 28
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |Count_of_Destination_Entries |   Length_of_Reserved_Field    | 32
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                                                             | 8 each
# |        List of Destination_Entries (variable length)        |
# |                                                             |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                                                             | 12
# /                 Options (variable length)                   /
# |                                                             |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                                                             |
# /                  Data (variable length)                     /
# |                                                             |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
#
class AddressPdu:
    def __init__(self):
        self.type = PduType.Address  # Either AddressPdu or ExtraAddressPdu Type */
        self.total = 0  # Total number of fragmented DataPDUs of the message
        self.cwnd = (
            0  # Number of PDUs which are sent in the current transmission window
        )
        self.seqnohi = 0  # Sequence Number of the last PDU in the current window
        self.src_ipaddr = ""  # Emitter address (IPv4 address)
        self.msid = 0  #  MSID is a unique identifier created by the transmitter.
        self.expires = 0  # Number of seconds since 00:00:00 1.1.1970 - UnixTime
        self.rsvlen = 0  # This field contains the length of the reserved field
        self.dst_entries = []  # This field is an array of destination entries
        self.tsval = 0  # Timestamp Value Option
        self.payload = bytearray()  # Data Buffer (Optional)

    def length(self):
        return (
            32
            + DESTINATION_ENTRY_LEN * len(self.dst_entries)
            + OPT_TS_VAL_LEN
            + len(self.payload)
        )

    def to_buffer(self):
        srcid = int_from_bytes(socket.inet_aton(self.src_ipaddr))
        packet = bytearray()
        packet.extend(
            struct.pack(
                "<HccHHHHHHIIIHH",
                self.length(),  # 0: lengthOfPDU
                bytes([0]),  # 1: priority
                bytes([int(self.type)]),  # 2: PDU Type
                self.total,  # 3: Total Number of PDUs
                0,  # 4: chksum
                self.cwnd,  # 5: current window
                self.seqnohi,  # 6: highest seqno of window
                self.length() - len(self.payload),  # 7: data-offset
                0,  # 8: reserved: Must be zero.
                srcid,  # 9: Source identifier
                self.msid,  # 10: Message identifier
                self.expires,  # 11: Expires in sec
                len(self.dst_entries),  # 12: count of dest entries
                self.rsvlen,  # 13: length of reserved field
            )
        )
        # append dest-entries
        for i, val in enumerate(self.dst_entries):
            destid = int_from_bytes(socket.inet_aton(val.dest_ipaddr))
            packet.extend(struct.pack("<II", destid, val.seqno))
        # append TLV tsval
        packet.extend(
            struct.pack("<ccHQ", bytes([int(Option.TsVal)]), bytes([12]), 0, self.tsval)
        )
        # append data to address-pdu
        packet.extend(self.payload)
        return packet

    def from_buffer(self, buffer):
        if len(buffer) < MINIMUM_ADDRESS_PDU_LEN:
            logging.error("RX: AddressPdu.from_buffer() FAILED with: Message to small")
            return 0
        # Read AddressPdu Header
        unpacked_data = struct.unpack(
            "<HccHHHHHHIIIHH", buffer[:MINIMUM_ADDRESS_PDU_LEN]
        )
        pdu_len = unpacked_data[0]
        self.type = PduType(int_from_bytes(unpacked_data[2]))
        self.total = unpacked_data[3]
        self.cwnd = unpacked_data[5]
        self.seqnohi = unpacked_data[6]
        offset = unpacked_data[7]
        reserved_len = unpacked_data[8]
        self.src_ipaddr = socket.inet_ntoa(int_to_bytes(unpacked_data[9]))
        self.msid = unpacked_data[10]
        self.expires = unpacked_data[11]
        total_entries = unpacked_data[12]
        if len(buffer) < offset:
            logging.error("RX: AddressPdu.from_buffer() FAILED with: Message to small")
            return 0
        if (
            len(buffer) - MINIMUM_ADDRESS_PDU_LEN
            < total_entries * DESTINATION_ENTRY_LEN
        ):
            logging.error("RX: AddressPdu.from_buffer() FAILED with: Message to small")
            return 0

        # Read Destination entries
        num_entries = 0
        nbytes = MINIMUM_ADDRESS_PDU_LEN
        while num_entries < total_entries:
            if nbytes + DESTINATION_ENTRY_LEN + reserved_len > offset:
                logging.error(
                    "RX: AddressPdu.from_buffer() FAILED with: Invalid DestinationEntry"
                )
                return 0
            dest_entry = DestinationEntry()
            consumed = dest_entry.from_buffer(buffer[nbytes:])
            if consumed < 0:
                logging.error(
                    "RX: AddressPdu.from_buffer() FAILED with: Invalid DestinationEntry"
                )
                return 0
            nbytes = nbytes + consumed
            num_entries = num_entries + 1
            self.dst_entries.append(dest_entry)

        # Read additional options
        while nbytes + 2 < offset:
            type = buffer[nbytes]
            optlen = buffer[nbytes + 1]
            if optlen == 0:
                logging.error(
                    "RX: AddressPdu.from_buffer() FAILED with: Invalid option TLV"
                )
                return 0
            if type == int(Option.TsVal):
                if optlen != 12:
                    logging.error(
                        "RX: AddressPdu.from_buffer() FAILED with: Invalid option len of tsVal"
                    )
                    return 0
                tlv_unpacked = struct.unpack("<ccHQ", buffer[nbytes : nbytes + 12])
                self.tsval = tlv_unpacked[3]
            else:
                logging.info("RX: Ignore unkown option {}".format(type))
            nbytes = nbytes + optlen

        # Save additional data
        self.payload = buffer[nbytes:pdu_len]
        return nbytes

    def find_addr(self, addr):
        for i, val in enumerate(self.dst_entries):
            if val.dest_ipaddr == addr:
                return True
        return False

    def get_dest_list(self):
        dsts = []
        for i, val in enumerate(self.dst_entries):
            dsts.append(val.dest_ipaddr)
        return dsts

    def log(self, rxtx):
        logging.debug(
            "{} *** {} *************************************************".format(
                rxtx, self.type
            )
        )
        logging.debug(
            "{} * total:{} cwnd:{} seqnoh:{} srcid:{} msid:{} expires:{} rsvlen:{}".format(
                rxtx,
                self.total,
                self.cwnd,
                self.seqnohi,
                self.src_ipaddr,
                self.msid,
                self.expires,
                self.rsvlen,
            )
        )
        for i, val in enumerate(self.dst_entries):
            logging.debug(
                "{} * dst[{}] dstid:{} seqno:{}".format(
                    rxtx, i, val.dest_ipaddr, val.seqno
                )
            )
        logging.debug("{} * tsval: {}".format(rxtx, self.tsval))
        logging.debug(
            "{} ****************************************************************".format(
                rxtx
            )
        )
