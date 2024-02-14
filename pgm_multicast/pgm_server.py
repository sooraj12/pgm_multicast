import socket
import asyncio
import struct
import random

from .logger import logging
from .pdu import Pdu
from .constants import (
    MINIMUM_PACKET_LEN,
    PduType,
    FAMILY,
    ACK_PDU_DELAY_MSEC,
    MAX_ADDRESS_PDU_LEN,
)
from .config import pgm_cfg
from enum import Enum
from .address_pdu import AddressPdu
from datetime import datetime
from .utils import (
    calc_remaining_time,
    timedelta_milli,
    reassemble,
    calc_datarate,
    avg_datarate,
)
from .ack_pdu import AckPdu, AckInfoEntry
from .data_pdu import DataPdu
from .os_mcast_utils import set_multicast_if, is_windows, make_ip_mreqn_struct


class ServerEvents(Enum):
    AddressPdu = 2
    ExtraAddressPdu = 3
    DataPdu = 4
    LastPduTimeout = 5
    AckPduTimeout = 6


class ServerStates(Enum):
    Idle = 0
    ReceivingData = 1
    SentAck = 2
    Finished = 3


class ServerState:
    def __init__(self, cli, loop, config, my_ip, remote_ip, msid):
        self.config = config
        self.states = dict()
        self.states[ServerStates.Idle] = self.state_IDLE
        self.states[ServerStates.ReceivingData] = self.state_RECEIVING_DATA
        self.states[ServerStates.SentAck] = self.state_SENT_ACK
        self.states[ServerStates.Finished] = self.state_FINISHED
        self.curr = ServerStates.Idle
        self.cli = cli
        self.loop = loop

        # Address_PDU and Reception of DATA_PDUs
        self.total = 0  # The total number of PDUs of the message
        self.my_ipaddr = my_ip  # My own identifier
        self.remote_ipaddr = remote_ip  # The ipaddress of the sender
        self.dests = []  # List of destination IDs of message reception
        self.msid = msid  # Unique Message ID of transfer
        self.max_address_pdu_len = (
            0  # The maximum length of the AddressPdu - User to calc RoundTripTime
        )
        self.start_timestamp = None  # Timestamp at which 1st AddressPdu or 1st DataPdu of a RxPhase was received
        self.cwnd = 0  # Number of PDUs which should be received in the current RxPhase
        self.seqnohi = 0  # Highest Sequence Number of current window
        self.cwnd_seqno = 0  # Transmission Window sequence number
        self.ts_val = 0  # Timestamp Value from 1st AddressPdu
        self.tvalue = 0  # TValue which is sent with 1st AckPdu
        self.mtu = config["mtu"]  # Packet Size of received fragments

        # Fragments and AckStatus ###############################################
        self.fragments = dict()  # The list of received fragments
        self.received = dict()  # List of received fragments in the current RxPhase

        # LastPduTimer and AckPduTimer ##############################################
        self.rx_datarate = 0  # The rxDatarate which is used to calc the last PDU timer
        self.last_pdu_delay_timer = None  # Last PDU Timer for sending ACKs
        self.ack_pdu_timer = None  # ACK PDU Timer for retransmitting ACKs
        self.mcast_ack_timeout = 0  # Additional timeout for sending an ACK
        self.ack_retry_count = 0  # Current number of Ack retries
        self.max_ack_retry_count = config["max_ack_retry_count"]

    def tran(self, to):
        self.curr = to

    async def dispatch(self, ev):
        func = self.states[self.curr]
        if func is not None:
            await func(ev)

    def update_max_address_pdu_len(self, pdu_len):
        if pdu_len > self.max_address_pdu_len:
            self.max_address_pdu_len = pdu_len

    def save_fragment(self, seqno, buffer):
        if seqno not in self.fragments:
            self.fragments[seqno] = buffer
            logging.debug(
                "RCV | Saved fragment {} with len {}".format(seqno, len(buffer))
            )

    def last_pdu_delay_timeout(self):
        event = {"id": ServerEvents.LastPduTimeout}
        asyncio.ensure_future(self.dispatch(event))

    def cancel_last_pdu_delay_timer(self):
        if self.last_pdu_delay_timer is not None:
            self.last_pdu_delay_timer.cancel()
            self.last_pdu_delay_timer = None

    def start_last_pdu_delay_timer(self, timeout):
        if self.last_pdu_delay_timer is not None:
            self.cancel_last_pdu_delay_timer()
        timeout = timeout / 1000
        self.last_pdu_delay_timer = self.loop.call_later(
            timeout, self.last_pdu_delay_timeout
        )

    def calc_tvalue(self):
        deltatime = datetime.now() - self.start_timestamp
        tvalue = round(timedelta_milli(deltatime))
        logging.debug("RX TValue: {}".format(tvalue))
        return tvalue

    def get_missing_fragments(self):
        missing = []
        for i in range(0, self.seqnohi):
            if i not in self.fragments:
                missing.append(i)
        return missing

    def calc_ack_pdu_timeout(self, num_dests, rx_datarate):
        if rx_datarate == 0:
            rx_datarate = 5000  # just to be sure we have a valid value here
        message_len = MAX_ADDRESS_PDU_LEN
        timeout = self.config["rtt_extra_delay"]
        timeout += num_dests * ACK_PDU_DELAY_MSEC
        timeout += round(message_len * 8 * 1000 / rx_datarate)
        logging.debug("RX AckPduTimeout: {} num_dests: {}".format(timeout, num_dests))
        return timeout

    def ack_pdu_timeout(self):
        event = {"id": ServerEvents.AckPduTimeout}
        asyncio.ensure_future(self.dispatch(event))

    def cancel_ack_pdu_timer(self):
        if self.ack_pdu_timer is not None:
            self.ack_pdu_timer.cancel()
            self.ack_pdu_timer = None

    def start_ack_pdu_timer(self, timeout):
        if self.ack_pdu_timer is not None:
            self.cancel_ack_pdu_timer()
        timeout = timeout / 1000
        self.ack_pdu_timer = self.loop.call_later(timeout, self.ack_pdu_timeout)

    def send_ack(self, seqnohi, missing, tvalue, ts_ecr):
        ack_pdu = AckPdu()
        ack_pdu.src_ipaddr = self.my_ipaddr
        ack_info_entry = AckInfoEntry()
        ack_info_entry.seqnohi = seqnohi
        ack_info_entry.remote_ipaddr = self.remote_ipaddr
        ack_info_entry.msid = self.msid
        ack_info_entry.tvalue = tvalue
        ack_info_entry.tsecr = ts_ecr
        ack_info_entry.missing_seqnos = missing
        ack_pdu.info_entries.append(ack_info_entry)
        pdu = ack_pdu.to_buffer()
        logging.debug(
            "RCV | {} send AckPdu to {} of len {} with ts_ecr: {} tvalue: {} seqnohi: {} missing: {}".format(
                self.my_ipaddr,
                self.remote_ipaddr,
                len(pdu),
                ts_ecr,
                tvalue,
                seqnohi,
                missing,
            )
        )
        self.cli.transport.sendto(pdu, (self.remote_ipaddr, self.cli.aport))

    def get_remaining_fragments(self, seqno):
        remaining = 0

        if self.total <= 0:
            logging.debug(
                "RCV | Remaining fragment: 0 - Total number of PDUs is unkown"
            )
            return 0
        # Calculate remaining number of PDUs based on the total number of
        # PDUs in the current window and the already received number of PDUS */
        remaining_window_num = self.cwnd - len(self.received)
        # Calculate the remaining number of PDUS based on the sequence number
        # of the current received PDU and the highest sequence number of the current window */
        remaining_window_seqno = self.seqnohi - seqno
        # The smallest number will become the remaining number of PDUs */
        remaining = min(remaining_window_num, remaining_window_seqno)
        remaining = max(remaining, 0)
        logging.debug(
            "RCV | Remaining: {} windowNum: {} window_seqno: {}".format(
                remaining, remaining_window_num, remaining_window_seqno
            )
        )
        return remaining

    def update_rx_datarate(self, seqno):
        datarate = 0
        nbytes = 0

        if self.total > 1:
            remaining = self.get_remaining_fragments(seqno)
            nbytes = (self.cwnd - remaining) * self.mtu
        else:
            for key, val in self.received.items():
                nbytes += val
        # Calculate average datarate since 1st received DataPdu
        datarate = calc_datarate(self.start_timestamp, nbytes)
        # Weight new datarate with old datarate
        if self.rx_datarate == 0:
            self.rx_datarate = datarate
        datarate = avg_datarate(self.rx_datarate, datarate)
        # Update the stored datarate
        self.rx_datarate = min(datarate, self.config["max_datarate"])  # upper boundary
        self.rx_datarate = max(datarate, self.config["min_datarate"])  # lower boundary
        logging.debug(
            "RCV | {} Updated RxDatarate to {} bit/s".format(
                self.my_ipaddr, self.rx_datarate
            )
        )

    def update_mtu_size(self):
        mtu_size = 0
        for key, val in self.received.items():
            if val > mtu_size:
                mtu_size = val
        if mtu_size != 0:
            self.mtu = mtu_size

    async def state_IDLE(self, ev):
        logging.debug("RCV | state_IDLE: {}".format(ev["id"]))
        match ev["id"]:
            case ServerEvents.AddressPdu:
                # Initialize Reception Phase
                address_pdu = ev["address_pdu"]
                self.dests = address_pdu.get_dest_list()
                self.total = address_pdu.total
                self.start_timestamp = datetime.now()
                self.cwnd = address_pdu.cwnd
                self.seqnohi = address_pdu.seqnohi
                self.ts_val = address_pdu.tsval
                self.tvalue = 0
                self.received = {}
                self.ack_retry_count = 0
                self.update_max_address_pdu_len(address_pdu.length())
                self.log()

                if len(address_pdu.payload) > 0:
                    # TrafficMode.Message
                    self.received[0] = len(address_pdu.payload)
                    self.save_fragment(0, address_pdu.payload)

                    # Multicast - Wait random time before sending AckPdu
                    self.mcast_ack_timeout = round(
                        random.random() * (len(self.dests) * ACK_PDU_DELAY_MSEC)
                    )
                    logging.debug(
                        "RCV | start LAST_PDU timer with a {} msec timeout')".format(
                            self.mcast_ack_timeout
                        )
                    )
                    self.start_last_pdu_delay_timer(self.mcast_ack_timeout)
                    logging.debug("RCV | IDLE - Change state to RECEIVING_DATA")
                    self.tran(ServerStates.ReceivingData)

                else:
                    # TrafficMode.Bulk - Start Last PDU Timer
                    remaining_bytes = (
                        address_pdu.cwnd + 1
                    ) * self.mtu  # Additional AddressPdu
                    if self.rx_datarate == 0:
                        timeout = calc_remaining_time(remaining_bytes, 5000)
                    else:
                        timeout = calc_remaining_time(remaining_bytes, self.rx_datarate)

                    # At multicast the AckPdu will be randomly delayed to avoid collisions
                    self.mcast_ack_timeout = round(
                        random.random() * (len(self.dests) * ACK_PDU_DELAY_MSEC)
                    )
                    timeout += self.mcast_ack_timeout
                    logging.debug(
                        "RCV | start LAST_PDU timer with a {} msec timeout".format(
                            timeout
                        )
                    )
                    self.start_last_pdu_delay_timer(timeout)
                    # Change state to RECEIVING_DATA
                    logging.debug("RCV | IDLE - change state to RECEIVING_DATA")
                    self.tran(ServerStates.ReceivingData)

            case ServerEvents.ExtraAddressPdu:
                # Initialize Reception Phase
                address_pdu = ev["address_pdu"]
                self.dests = address_pdu.get_dest_list()
                self.total = address_pdu.total
                self.start_timestamp = datetime.now()
                self.cwnd = address_pdu.cwnd
                self.seqnohi = address_pdu.seqnohi
                self.tvalue = 0
                self.received = {}
                self.ack_retry_count = 0
                self.update_max_address_pdu_len(address_pdu.length())
                self.log()

                # Multicast - Wait random time before sending AckPdu
                self.mcast_ack_timeout = round(
                    random.random() * (len(self.dests) * ACK_PDU_DELAY_MSEC)
                )
                logging.debug(
                    "RCV | start LAST_PDU timer with a {} msec timeout')".format(
                        self.mcast_ack_timeout
                    )
                )
                self.start_last_pdu_delay_timer(self.mcast_ack_timeout)
                logging.debug("RCV | IDLE - Change state to RECEIVING_DATA")
                self.tran(ServerStates.ReceivingData)

            case ServerEvents.DataPdu:
                # Initialize Reception Phase
                data_pdu = ev["data_pdu"]
                self.start_timestamp = datetime.now()
                self.cwnd = data_pdu.cwnd
                self.seqnohi = data_pdu.seqnohi
                self.ts_val = 0
                self.tvalue = 0
                self.received = {}
                self.ack_retry_count = 0
                self.log()
                # Change state to RECEIVING_DATA */
                logging.debug("RCV | change state to RECEIVING_DATA")
                self.tran(ServerStates.ReceivingData)

            case _:
                pass

    async def state_RECEIVING_DATA(self, ev):
        logging.debug("RCV | state_RECEIVING_DATA: {}".format(ev["id"]))

        match ev["id"]:
            case ServerEvents.AddressPdu:
                self.cancel_last_pdu_delay_timer()

                # Initialize Reception Phase
                address_pdu = ev["address_pdu"]
                self.dests = address_pdu.get_dest_list()
                self.total = address_pdu.total
                self.start_timestamp = datetime.now()
                self.cwnd = address_pdu.cwnd
                self.seqnohi = address_pdu.seqnohi
                self.ts_val = address_pdu.tsval
                self.tvalue = 0
                self.received = {}
                self.ack_retry_count = 0
                self.update_max_address_pdu_len(address_pdu.length())
                self.log()

                # TrafficMode.Bulk - Start Last PDU Timer
                remaining_bytes = (
                    address_pdu.cwnd + 1
                ) * self.mtu  # Additional AddressPdu
                if self.rx_datarate == 0:
                    timeout = calc_remaining_time(remaining_bytes, 5000)
                else:
                    timeout = calc_remaining_time(remaining_bytes, self.rx_datarate)
                if len(self.dests) > 1:
                    # At multicast the AckPdu will be randomly delayed to avoid collisions
                    self.mcast_ack_timeout = round(
                        random.random() * (len(self.dests) * ACK_PDU_DELAY_MSEC)
                    )
                    timeout += self.mcast_ack_timeout
                logging.debug(
                    "RCV | start LAST_PDU timer with a {} msec timeout".format(timeout)
                )
                self.start_last_pdu_delay_timer(timeout)

            case ServerEvents.DataPdu:
                self.cancel_last_pdu_delay_timer()
                data_pdu = ev["data_pdu"]
                # Save the received data fragment
                self.received[data_pdu.seqno] = len(data_pdu.data)
                self.save_fragment(data_pdu.seqno, data_pdu.data)
                # Update the transmission window parameters
                self.cwnd = data_pdu.cwnd
                self.seqnohi = data_pdu.seqnohi
                self.cwnd_seqno = data_pdu.cwnd_seqno
                # Calculate rx-datarate
                self.update_rx_datarate(data_pdu.seqno)

                if self.cwnd > 0:
                    # Calculate how many data PDUs are still awaited
                    remaining = self.get_remaining_fragments(data_pdu.seqno)
                    logging.debug(
                        "RCV | {} | Received DataPDU[{}] Remaining: {} RxDatarate: {} bit/s".format(
                            self.my_ipaddr, data_pdu.seqno, remaining, self.rx_datarate
                        )
                    )
                    # Start LAST PDU Timer
                    remaining += 1  # Additional ExtrAddressPdu
                    timeout = calc_remaining_time(
                        remaining * self.mtu, self.rx_datarate
                    )
                    # At multicast the AckPdu will be randomly delayed to avoid collisions
                    self.mcast_ack_timeout = round(
                        random.random() * (len(self.dests) * ACK_PDU_DELAY_MSEC)
                    )
                    timeout += self.mcast_ack_timeout
                    logging.debug(
                        "RCV | start LAST_PDU timer with a {} msec timeout".format(
                            timeout
                        )
                    )
                    self.start_last_pdu_delay_timer(timeout)

            case ServerEvents.LastPduTimeout:
                self.cancel_last_pdu_delay_timer()

                self.tvalue = self.calc_tvalue()
                self.tvalue = self.tvalue - self.mcast_ack_timeout
                missed = self.get_missing_fragments()
                self.send_ack(self.seqnohi, missed, self.tvalue, self.ts_val)
                timeout = self.calc_ack_pdu_timeout(len(self.dests), self.rx_datarate)
                logging.debug(
                    "RCV | Start ACK_PDU timer with a {} msec timeout".format(timeout)
                )
                self.start_ack_pdu_timer(timeout)
                # Change state to SENT_ACK
                self.cwnd = 00  # Reset RxPhase
                logging.debug("RCV | RECEIVING_DATA - Change state to SENT_ACK")
                self.tran(ServerStates.SentAck)

            case ServerEvents.ExtraAddressPdu:
                self.cancel_last_pdu_delay_timer()
                address_pdu = ev["address_pdu"]
                # Update Reception Phase
                self.dests = address_pdu.get_dest_list()
                self.total = address_pdu.total
                self.cwnd = address_pdu.cwnd
                self.seqnohi = address_pdu.seqnohi
                self.tvalue = 0
                self.log("RCV")

                # Multicast - Wait random time before sending AckPdu
                self.mcast_ack_timeout = round(
                    random.random() * (len(self.dests) * ACK_PDU_DELAY_MSEC)
                )
                logging.debug(
                    "RCV | start LAST_PDU timer with a {} msec timeout')".format(
                        self.mcast_ack_timeout
                    )
                )
                self.start_last_pdu_delay_timer(self.mcast_ack_timeout)

            case ServerEvents.AckPduTimeout:
                self.cancel_ack_pdu_timer()

            case _:
                pass

    async def state_SENT_ACK(self, ev):
        logging.debug("RCV | state_SENT_ACK: {}".format(ev["id"]))

        match ev["id"]:
            case ServerEvents.AddressPdu:
                self.cancel_ack_pdu_timer()
                address_pdu = ev["address_pdu"]

                to_me = address_pdu.find_addr(self.my_ipaddr)
                if to_me is False:
                    # Change state to FINISHED
                    logging.debug("SND | change state to FINISHED")
                    self.tran(ServerStates.Finished)
                    message = reassemble(self.fragments)
                    self.cli.on_finished(self.msid, message, self.remote_ipaddr)
                    return

                self.dests = address_pdu.get_dest_list()
                self.total = address_pdu.total
                self.start_timestamp = datetime.now()
                self.cwnd = address_pdu.cwnd
                self.seqnohi = address_pdu.seqnohi
                self.ts_val = address_pdu.tsval
                self.tvalue = 0
                self.received = {}
                self.ack_retry_count = 0
                self.update_max_address_pdu_len(address_pdu.length())
                self.log()

                if len(address_pdu.payload) > 0:
                    self.received[0] = len(address_pdu.payload)
                    self.save_fragment(0, address_pdu.payload)
                    # Multicast - Wait random time before sending AckPdu
                    self.mcast_ack_timeout = round(
                        random.random() * (len(self.dests) * ACK_PDU_DELAY_MSEC)
                    )
                    logging.debug(
                        "RCV | start LAST_PDU timer with a {} msec timeout')".format(
                            self.mcast_ack_timeout
                        )
                    )
                    self.start_last_pdu_delay_timer(self.mcast_ack_timeout)
                    logging.debug("RCV | IDLE - Change state to RECEIVING_DATA")
                    self.tran(ServerStates.ReceivingData)
                else:
                    # TrafficMode.Bulk - Start Last PDU Timer
                    remaining_bytes = (
                        address_pdu.cwnd + 1
                    ) * self.mtu  # Additional AddressPdu
                    timeout = calc_remaining_time(remaining_bytes, self.rx_datarate)
                    if len(self.dests) > 1:
                        # At multicast the AckPdu will be randomly delayed to avoid collisions
                        self.mcast_ack_timeout = round(
                            random.random() * (len(self.dests) * ACK_PDU_DELAY_MSEC)
                        )
                        timeout += self.mcast_ack_timeout
                    logging.debug(
                        "RCV | start LAST_PDU timer with a {} msec timeout".format(
                            timeout
                        )
                    )
                    self.start_last_pdu_delay_timer(timeout)
                    # Change state to RECEIVING_DATA
                    logging.debug("RCV | IDLE - change state to RECEIVING_DATA")

                    self.tran(ServerStates.ReceivingData)

            case ServerEvents.DataPdu:
                self.cancel_ack_pdu_timer()
                data_pdu = ev["data_pdu"]
                # Save the received data fragment
                self.received[data_pdu.seqno] = len(data_pdu.data)
                self.save_fragment(data_pdu.seqno, data_pdu.data)
                self.update_mtu_size()

                if data_pdu.cwnd_seqno > self.cwnd_seqno:
                    # Initialize new Reception Phase on DataPdu */
                    self.start_timestamp = datetime.now()
                    self.cwnd = data_pdu.cwnd
                    self.seqnohi = data_pdu.seqnohi
                    self.ts_val = 0
                    self.tvalue = 0
                    self.received = {}
                    self.ack_retry_count = 0
                else:
                    self.update_rx_datarate(data_pdu.seqno)
                self.log()

                if self.cwnd > 0:
                    # Calculate how many data PDUs are still awaited
                    remaining = self.get_remaining_fragments(data_pdu.seqno)
                    logging.debug(
                        "RX {} | Received DataPDU[{}] Remaining: {} RxDatarate: {} bit/s".format(
                            self.my_ipaddr, data_pdu.seqno, remaining, self.rx_datarate
                        )
                    )
                    # Start LAST PDU Timer
                    remaining += 1  # Additional ExtrAddressPdu
                    timeout = calc_remaining_time(
                        remaining * self.mtu, self.rx_datarate
                    )
                    if len(self.dests) > 1:
                        # At multicast the AckPdu will be randomly delayed to avoid collisions
                        self.mcast_ack_timeout = round(
                            random.random() * (len(self.dests) * ACK_PDU_DELAY_MSEC)
                        )
                        timeout += self.mcast_ack_timeout
                    logging.debug(
                        "RCV | start LAST_PDU timer with a {} msec timeout".format(
                            timeout
                        )
                    )
                    self.start_last_pdu_delay_timer(timeout)
                    # Change state to RECEIVING_DATA
                    logging.debug("RCV | IDLE - change state to RECEIVING_DATA")
                    self.tran(ServerStates.ReceivingData)

            case ServerEvents.LastPduTimeout:
                self.cancel_last_pdu_delay_timer()

            case ServerEvents.AckPduTimeout:
                self.cancel_ack_pdu_timer()
                self.ack_retry_count += 1
                logging.debug(
                    "RCV | RetryCount: {} MaxRetryCount: {}".format(
                        self.ack_retry_count, self.cfg["max_ack_retry_count"]
                    )
                )
                if self.ack_retry_count > self.config["max_ack_retry_count"]:
                    logging.debug(
                        "Aborted reception due: Maximum Ack retry count reached"
                    )
                else:
                    # Resend AckPdu
                    self.send_ack(
                        self.seqnohi,
                        self.get_missing_fragments(),
                        self.tvalue,
                        self.ts_val,
                    )
                    timeout = self.calc_ack_pdu_timeout(
                        len(self.dests), self.rx_datarate
                    )
                    for i in range(0, self.ack_retry_count):
                        timeout = timeout * 2
                    logging.debug(
                        "RCV | Start ACK_PDU timer with a {} msec timeout".format(
                            timeout
                        )
                    )
                    self.start_ack_pdu_timer(timeout)

            case _:
                pass

    async def state_FINISHED(self, ev):
        logging.debug("RCV | state_FINISHED: {}".format(ev["id"]))
        self.cancel_last_pdu_delay_timer()
        self.cancel_ack_pdu_timer()

    def log(self):
        logging.debug(
            "RCV +--------------------------------------------------------------+"
        )
        logging.debug(
            "RCV | RX Phase                                                     |"
        )
        logging.debug(
            "RCV +--------------------------------------------------------------+"
        )
        logging.debug("RCV | remote_ipaddr: {}".format(self.remote_ipaddr))
        logging.debug("RCV | dests: {}".format(self.dests))
        logging.debug("RCV | cwnd: {}".format(self.cwnd))
        logging.debug("RCV | total: {}".format(self.total))
        logging.debug("RCV | seqnohi: {}".format(self.seqnohi))
        logging.debug("RCV | tsVal: {}".format(self.ts_val))
        logging.debug("RCV | rx_datarate: {}".format(self.rx_datarate))
        logging.debug(
            "RCV +--------------------------------------------------------------+"
        )


# server
class Server:
    def __init__(self, loop, config, observer):
        self.loop = loop
        self.src_id = socket.inet_aton(config["src_ipaddr"])
        self.src_ipaddr = config["src_ipaddr"]
        self.mcast_ttl = config["mcast_ttl"]
        self.mcast_ipaddr = config["mcast_ipaddr"]
        self.dport = config["dport"]
        self.aport = config["aport"]
        self.rx_contexts = dict()
        self.transport = None
        self.iface_index = config["iface_index"]
        self.observer = observer

        # create socket
        self.sock = socket.socket(family=FAMILY, type=socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.mcast_ttl)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, False)
        set_multicast_if(
            self.sock, self.iface_index, self.mcast_ipaddr, self.src_ipaddr
        )
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if is_windows:
            self.sock.bind((self.src_ipaddr, self.dport))
            mreq_bytes = struct.pack(
                "!4sI", socket.inet_aton(self.mcast_ipaddr), self.iface_index
            )
            self.sock.setsockopt(
                socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq_bytes
            )
        else:
            self.sock.bind((self.mcast_ipaddr, self.dport))
            self.sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                make_ip_mreqn_struct(
                    self.mcast_ipaddr, self.iface_index, self.src_ipaddr
                ),
            )

        logging.debug("socket listens for data in port: {}".format(self.dport))

        self.task = asyncio.ensure_future(self.start())

    async def start(self):
        coro = self.loop.create_datagram_endpoint(lambda: self, sock=self.sock)
        await asyncio.wait_for(coro, 2)

    def connection_made(self, transport):
        self.transport = transport
        logging.debug("Connection is made:")

    def on_finished(self, msid, message, src_addr):
        logging.debug(
            "RCV | Received message {} of len {} from {}".format(
                msid, len(message), src_addr
            )
        )
        self.observer.message_received(message)

    def on_data_pdu(self, data, remote):
        data_pdu = DataPdu()
        data_pdu.from_buffer(data)

        remote_ipaddr = data_pdu.src_ipaddr
        msid = data_pdu.msid
        key = (remote_ipaddr, msid)

        if key not in self.rx_contexts:
            pass
        else:
            # There is already a RxContext -> forward message to statemachine
            ctx = self.rx_contexts[key]
            data_pdu.log("RCV")
            ev = dict()
            ev["id"] = ServerEvents.DataPdu
            ev["data_pdu"] = data_pdu
            asyncio.ensure_future(ctx.dispatch(ev))

    def on_address_pdu(self, data):
        address_pdu = AddressPdu()
        address_pdu.from_buffer(data)

        if address_pdu.type == PduType.ExtraAddress:
            event_id = ServerEvents.ExtraAddressPdu
        else:
            event_id = ServerEvents.AddressPdu

        # On receipt of an Address_PDU the receiving node shall first check whether
        # the Address_PDU with the same tuple "Source_ID, MSID" has already been received
        remote_ipaddr = address_pdu.src_ipaddr
        msid = address_pdu.msid
        key = (remote_ipaddr, msid)

        if key not in self.rx_contexts:
            # If its own ID is not in the list of Destination_Entries,
            # the receiving node shall discard the Address_PDU
            if address_pdu.find_addr(self.src_ipaddr) is False:
                return
            # A new RxContext needs to be initialized
            address_pdu.log("RCV")
            ctx = ServerState(
                self,
                self.loop,
                pgm_cfg,
                self.src_ipaddr,
                remote_ipaddr,
                address_pdu.msid,
            )
            self.rx_contexts[key] = ctx
            ev = dict()
            ev["id"] = event_id
            ev["address_pdu"] = address_pdu
            asyncio.ensure_future(ctx.dispatch(ev))
        else:
            # There is already a RxContext -> forward message to statemachine
            address_pdu.log("RCV")
            ctx = self.rx_contexts[key]
            ev = dict()
            ev["id"] = event_id
            ev["address_pdu"] = address_pdu
            asyncio.ensure_future(ctx.dispatch(ev))

    def datagram_received(self, data, addr):
        pdu = Pdu()
        pdu.from_buffer(data[:MINIMUM_PACKET_LEN])
        pdu.log("RX")

        logging.debug(
            "RX Received packet from {} type: {} len:{}".format(addr, pdu.type, pdu.len)
        )

        if pdu.type == int(PduType.Address):
            self.on_address_pdu(data)
        elif pdu.type == int(PduType.ExtraAddress):
            self.on_address_pdu(data, addr)
        elif pdu.type == int(PduType.Data):
            self.on_data_pdu(data, addr)
        else:
            logging.warn("Received unkown PDU type {}".format(pdu.type))

    def error_received(self, err):
        logging.error("Error received:", err)

    def connection_lost():
        logging.debug("Socket closed, stop the event loop")
