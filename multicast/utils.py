import ctypes
import sys
import socket
import platform
import struct

# os specific configurations
is_windows = platform.system() == "Windows"

if sys.platform == "win32":
    iphlpapi = ctypes.WinDLL("iphlpapi")
    win32_GetAdapterIndex = iphlpapi.GetAdapterIndex
    win32_GetAdapterIndex.argtypes = [ctypes.c_wchar_p, ctypes.POINTER(ctypes.c_ulong)]


def iface_name_to_index(iface_name):
    if sys.platform == "win32":
        if_idx = ctypes.c_ulong()
        iface_name_string = ctypes.c_wchar_p("\\DEVICE\\TCPIP_" + iface_name)
        win32_GetAdapterIndex(iface_name_string, ctypes.byref(if_idx))
        return if_idx.value
    else:
        return socket.if_nametoindex(iface_name)


def make_ip_mreqn_struct(mcast_ip, iface_index, iface_ip):
    return struct.pack(
        "@4s4si",
        socket.inet_aton(mcast_ip),
        socket.inet_aton(iface_ip),
        iface_index,
    )


def set_multicast_if(msocket, iface_index, mcast_ip, iface_ip):
    if is_windows:
        msocket.setsockopt(
            socket.IPPROTO_IP, socket.IP_MULTICAST_IF, struct.pack("!I", iface_index)
        )
    else:
        msocket.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_MULTICAST_IF,
            make_ip_mreqn_struct(mcast_ip, iface_index, iface_ip),
        )
