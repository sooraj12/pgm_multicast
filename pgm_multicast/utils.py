from datetime import datetime
from .logger import logging


def fragment(data, size):
    fragments = []
    curr = 0
    while curr < len(data):
        if len(data[curr:]) > size:
            fragments.append(data[curr : curr + size])
            curr += size
        else:
            fragments.append(data[curr:])
            curr += len(data[curr:])
    return fragments


# Convert a byte into an integer
def int_from_bytes(xbytes):
    return int.from_bytes(xbytes, "big")


# Convert an integer into a byte
def int_to_bytes(x):
    return x.to_bytes((x.bit_length() + 7) // 8, "big")


def get_seqnohi(tx_fragments):
    seqno = 0
    for key in tx_fragments:
        seqno = key
    return seqno


# Convert a datetime into milliseconds
def date_to_milli(date):
    if isinstance(date, datetime):
        epoch = datetime.utcfromtimestamp(0)
        return round((date - epoch).total_seconds() * 1000.0)


def get_cwnd(cwnd_list, air_datarate, DEFAULT_AIR_DATARATE):
    for i, val in enumerate(cwnd_list):
        if air_datarate <= val["datarate"]:
            return val["cwnd"]
    return DEFAULT_AIR_DATARATE


# Returns a dict() of all unacked fragment sizes
def unacked_fragments(dest_status_list, fragments, cwnd):
    unacked_list = dict()
    curr = 0
    for i, val in enumerate(fragments):
        for addr in dest_status_list:
            if dest_status_list[addr].fragment_ack_status[i] is False:
                if i not in unacked_list:
                    unacked_list[i] = dict()
                    unacked_list[i]["sent"] = False
                    unacked_list[i]["len"] = len(fragments[i])
                    curr += len(fragments[i])
                    if curr >= cwnd:
                        # Reached window limit
                        logging.debug(
                            "TX unacked_fragments: {} cwnd: {} curr: {}".format(
                                unacked_list, cwnd, curr
                            )
                        )
                        return unacked_list
    logging.debug("TX unacked_fragments: {}".format(unacked_list))
    return unacked_list


def timedelta_milli(td):
    return td.days * 86400000 + td.seconds * 1000 + td.microseconds / 1000


# Calculate the time period to wait for the remaining bytes
def calc_remaining_time(remaining_bytes, rx_datarate):
    if rx_datarate == 0:
        rx_datarate = 5000  # just to be sure we have a value here
    msec = 1000.0 * remaining_bytes * 8 / rx_datarate
    logging.info(
        "RCV | Remaining Time {} msec - Payload {} bytes - AirDatarate: {} bit/s".format(
            round(msec), remaining_bytes, rx_datarate
        )
    )
    return round(msec)


# Calculate datarate based on a start timestamp and the received bytes
def calc_datarate(ts, bytes):
    datarate = 0
    d = datetime.now() - ts
    msec = timedelta_milli(d)
    if msec > 0:
        datarate = (float)(bytes * 8 * 1000) / msec
    logging.info(
        "RCV | Datarate(): {} Received bytes: {} Interval: {}".format(
            int(datarate), bytes, msec
        )
    )
    return round(datarate)


def message_len(fragments):
    bytes = 0
    for i, val in enumerate(fragments):
        bytes = bytes + len(val)
    return bytes


def get_sent_bytes(fragments):
    bytes = 0
    for key in fragments:
        bytes = bytes + fragments[key]["len"]
    return bytes


def received_all_acks(dest_status_list):
    for addr in dest_status_list:
        if dest_status_list[addr].ack_received is False:
            return False
    return True


# Reassemble dictionary of fragments into a complete message
def reassemble(fragments):
    message = bytearray()
    for i, val in fragments.items():
        message.extend(val)

    return message


# Calculate an average datarate. Weight-Factor: 50%
def avg_datarate(old, new):
    avg = old * 0.50 + new * 0.50
    logging.info(
        "RCV | Avg_datarate() old: {} bit/s new: {} bit/s avg: {} bit/s".format(
            old, new, avg
        )
    )
    return round(avg)
