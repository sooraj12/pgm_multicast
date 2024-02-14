from .constants import INTERFACE_IP, IFACE_INDEX

# transport config
transport_conf = dict()
transport_conf["src_ipaddr"] = INTERFACE_IP
transport_conf["mcast_ipaddr"] = "234.0.0.1"
transport_conf["mcast_ttl"] = 32
transport_conf["dport"] = 20000  # data port
transport_conf["aport"] = 22000  # ack port
transport_conf["iface_index"] = IFACE_INDEX

# pgm config
pgm_cfg = dict()
pgm_cfg["mtu"] = 1024
pgm_cfg["inflight_bytes"] = 3000
pgm_cfg["initial_cwnd"] = 5000
pgm_cfg["default_datarate"] = 50000
pgm_cfg["min_datarate"] = 250
pgm_cfg["max_datarate"] = 800000
pgm_cfg["max_increase"] = 0.25
pgm_cfg["max_decrease"] = 0.75
pgm_cfg["max_ack_retry_count"] = 3
pgm_cfg["max_ack_timeout"] = 60000
pgm_cfg["min_retry_timeout"] = 250
pgm_cfg["max_retry_timeout"] = 12000
pgm_cfg["max_retry_count"] = 3
pgm_cfg["max_missed_acks"] = 3
pgm_cfg["rtt_extra_delay"] = 1000
pgm_cfg["cwnds"] = [
    {"datarate": 3000, "cwnd": 5000},
    {"datarate": 6000, "cwnd": 10000},  # 3000 < x < 6000
    {"datarate": 9000, "cwnd": 20000},
    {"datarate": 40000, "cwnd": 25000},
    {"datarate": 80000, "cwnd": 50000},
    {"datarate": 160000, "cwnd": 100000},
    {"datarate": 800000, "cwnd": 100000},
]
