import socket
from enum import Enum, IntEnum
from .os_mcast_utils import get_interface_ip

# socket
FAMILY = socket.AF_INET
INTERFACE_IP, IFACE_INDEX = get_interface_ip(FAMILY)
# destination nodes
DEFAULT_AIR_DATARATE = 5000
DEFAULT_ACK_TIMEOUT = 10
DEFAULT_RETRY_TIMEOUT = 1000
# pgm
MIN_PDU_DELAY = 10  # msec
ACK_PDU_DELAY_MSEC = 500  # 500sec
MINIMUM_PACKET_LEN = 4
MINIMUM_ADDRESS_PDU_LEN = 32  # The minimum length of an address-pdu
DESTINATION_ENTRY_LEN = 8  # The length of a destination entry PDU
OPT_TS_VAL_LEN = 12
MIN_BULK_SIZE = 512
# Assumption of the maximum AddressPdu length. Is used to calculate the AckPduTimeout
MAX_ADDRESS_PDU_LEN = 100
# Assumption of the maximum AckPdu length. Is used to calculate the AckPduTimeout
MAX_ACK_PDU_LEN = 100
MINIMUM_ACK_PDU_LEN = 14
MINIMUM_ACK_INFO_ENTRYLEN = 24
# The length of the Data PDU
DATA_PDU_HDRLEN = 24


# traffic types
class Traffic(Enum):
    Message = 0
    Bulk = 1


# PDU types
class PduType(IntEnum):
    Data = 0
    Ack = 1
    Address = 2
    Unkown = 3
    ExtraAddress = 4


class Option(IntEnum):
    TsVal = 0
    TsEcr = 1
