import socket
import netifaces

from .utils import iface_name_to_index

FAMILY = socket.AF_INET
GROUP_IP = "234.0.0.1"
PORT = 20000
BUFFER_SIZE = 1024
BUFFER_OFFSET = 400
HEADER = b"\x04\x02\x01\x03"
RATE_LIMIT = 15
SLEEP_TIME = 1  # seconds

gateways = netifaces.gateways()
default_gateway = gateways["default"][FAMILY]
default_gateway_iface = default_gateway[1]
interface_addresses = netifaces.ifaddresses(default_gateway_iface)
INTERFACE_IP = interface_addresses[FAMILY][0]["addr"]

iface_index = iface_name_to_index(default_gateway_iface)
