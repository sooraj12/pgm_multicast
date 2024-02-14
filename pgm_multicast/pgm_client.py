import asyncio
import socket
import random
import struct

from .constants import (
    FAMILY,
    Traffic,
    DEFAULT_ACK_TIMEOUT,
    DEFAULT_AIR_DATARATE,
    DEFAULT_RETRY_TIMEOUT,
    MIN_PDU_DELAY,
    PduType,
    ACK_PDU_DELAY_MSEC,
    MINIMUM_PACKET_LEN,
)
from .logger import logging
from .config import pgm_cfg
from enum import Enum
from .utils import (
    fragment,
    date_to_milli,
    get_seqnohi,
    get_cwnd,
    unacked_fragments,
    timedelta_milli,
    calc_datarate,
    message_len,
    get_sent_bytes,
    received_all_acks,
)
from datetime import datetime, timedelta
from .address_pdu import AddressPdu, DestinationEntry
from .pdu import Pdu
from .ack_pdu import AckPdu
from .data_pdu import DataPdu
from .os_mcast_utils import set_multicast_if, is_windows, make_ip_mreqn_struct


class Destination:
    def __init__(
        self, cfg, dest, num_fragments, air_datarate, retry_timeout, ack_timeout
    ):
        self.dest = dest  # Destination Identifier
        self.completed = False  # Transfer Status
        self.cfg = cfg
        # Ack Status
        self.ack_received = False  # Flag indicating if an ACK was received
        self.last_received_tsecr = 0  # Received echoed timestamp from the last AckPdu
        self.sent_data_count = 0  # Number of sent DataPDUs
        self.missed_data_count = 0  # Total number of reported missed DataPdus
        self.missed_ack_count = 0  # Number of consecutive missed Acks
        self.fragment_ack_status = dict()  # 0: false, 1: true,
        for i in range(0, num_fragments):
            self.fragment_ack_status[i] = False
        # Measurement
        self.air_datarate = air_datarate  # Measured AirDatarate for bulk traffic
        self.retry_timeout = (
            retry_timeout  # Measured retry timeout for message transfer
        )
        self.ack_timeout = ack_timeout  # RTT for AddressPdu and AckPdu back
        logging.debug("TX: Destination(to: {}):".format(dest))
        logging.debug("AirDatarate: {}".format(self.air_datarate))
        logging.debug("RetryTimeout: {}".format(self.retry_timeout))
        logging.debug("AckTimeout: {}".format(self.ack_timeout))
        # Loss detection
        self.missing_fragments = []

    def update_missed_data_cnt(self):
        for i, val in enumerate(self.missing_fragments):
            self.missed_data_count = self.missed_data_count + 1

    def is_completed(self):
        for key, val in self.fragment_ack_status.items():
            if val is False:
                return False
        return True

    def update_air_datarate_after_timeout(self):
        self.air_datarate = round(self.air_datarate / 2)
        self.air_datarate = min(
            self.air_datarate, self.cfg["max_datarate"]
        )  # upper boundary
        self.air_datarate = max(
            self.air_datarate, self.cfg["min_datarate"]
        )  # lower boundary
        logging.debug(
            "TX: Updated air_datarate for {} to {} after timeout".format(
                self.dest, self.air_datarate
            )
        )

    def update_fragment_ack_status(self, missing_fragments, seqnohi):
        # Mark all not missing DataPDUs as acked */
        # check if seqno should be 0 or 1 for one packet transmission
        for i in range(0, seqnohi + 1):
            if i in missing_fragments:
                self.fragment_ack_status[i] = False
            else:
                self.fragment_ack_status[i] = True
        self.missing_fragments = missing_fragments
        # Update the transfer status
        self.completed = self.is_completed()
        # Reset the missedAckCount
        self.missed_ack_count = 0

    def is_duplicate(self, tsecr):
        if tsecr is None or tsecr <= 0:
            logging.debug(
                "TX: Ignore invalid Timestamp Echo Reply for Duplicate Detection"
            )
            return False  # no duplicate
        if tsecr == self.last_received_tsecr:
            logging.debug("TX: AckPdu from {} is a duplicate".format(self.dest))
            return True  # is duplicate */
        logging.debug("Sender: Updated TSecr to {}".format(tsecr))
        self.last_received_tsecr = tsecr
        return False  # Not duplicate

    def update_ack_timeout(self, tsecr, tvalue):
        if tsecr is None or tsecr <= 0:
            logging.debug("TX: Ignore invalid Timestamp Echo Reply for AckTimeout")
            return
        if tvalue is None:
            tvalue = 0
        # Calculate the ack_timeout based on the RTT and TValue
        milli_now = date_to_milli(datetime.now())
        delivery_time = milli_now - tsecr
        new_ack_timeout = delivery_time - tvalue
        if new_ack_timeout <= 0:
            logging.debug("TX Ignore invalid ackTimeout of {}".format(new_ack_timeout))
            return
        self.ack_timeout = round((self.ack_timeout + new_ack_timeout) / 2)
        logging.debug(
            "TX: Updated AckTimeout for {} to {} new_ack_timeout: {} tValue: {} delivery_time: {}".format(
                self.dest, self.ack_timeout, new_ack_timeout, tvalue, delivery_time
            )
        )

    def update_retry_timeout(self, tsecr):
        if tsecr is None or tsecr <= 0:
            logging.debug("TX: Ignore invalid Timestamp Echo Reply for RetryTimeout")
            return
        # Calculate the retryTimeout based on the RTT */
        milli_now = date_to_milli(datetime.now())
        retry_timeout = milli_now - tsecr
        if retry_timeout <= 0:
            logging.error(
                "TX: Ignore invalid retry_timeout of {}".format(retry_timeout)
            )
            return
        retry_timeout = min(
            retry_timeout, self.cfg["max_retry_timeout"]
        )  # upper boundary
        retry_timeout = max(
            retry_timeout, self.cfg["min_retry_timeout"]
        )  # lower boundary
        self.retry_timeout = round((self.retry_timeout + retry_timeout) / 2)
        logging.debug(
            "TX: Updated retry_timeout for {} to {} new_retry_timeout: {}".format(
                self.dest, self.retry_timeout, retry_timeout
            )
        )

    def update_air_datarate(self, air_datarate):
        air_datarate = min(air_datarate, self.cfg["max_datarate"])  # upper boundary
        air_datarate = max(air_datarate, self.cfg["min_datarate"])  # lower boundary
        self.air_datarate = round((self.air_datarate + air_datarate) / 2)
        logging.debug(
            "TX: Updated air_datarate for {} to {} new_air_datarate: {}".format(
                self.dest, self.air_datarate, air_datarate
            )
        )

    def log(self):
        acked, total = 0, 0
        for key in self.fragment_ack_status:
            if self.fragment_ack_status[key] is True:
                acked = acked + 1
            total = total + 1
        logging.debug(
            "TX {}: {}/{} air_datarate: {} retry_timeout: {} loss: {} {}/{} missed-ack: {} missing: {}".format(
                self.dest,
                acked,
                total,
                self.air_datarate,
                self.retry_timeout,
                round(100 * self.missed_data_count / self.sent_data_count),
                self.missed_data_count,
                self.sent_data_count,
                self.missed_ack_count,
                self.missing_fragments,
            )
        )


class ClientEvents(Enum):
    Start = 2
    AckPdu = 4
    PduDelayTimeout = 5
    RetransmissionTimeout = 6


class ClientStates(Enum):
    Idle = 0
    SendingData = 1
    SendingExtraAddressPdu = 2
    WaitingForAcks = 3
    Finished = 4


class ClientState:
    def __init__(
        self,
        client,
        loop,
        config,
        dest_ip,
        dests,
        msid,
        message,
        traffic_type,
        observer,
    ):
        self.states = dict()
        self.states[ClientStates.Idle] = self.state_IDLE
        self.states[ClientStates.SendingData] = self.state_sending_data
        self.states[
            ClientStates.SendingExtraAddressPdu
        ] = self.state_sending_extra_address_pdu
        self.states[ClientStates.WaitingForAcks] = self.state_WAITING_FOR_ACKS
        self.states[ClientStates.Finished] = self.state_finished
        self.curr = ClientStates.Idle
        self.cli = client  # Pointer to client instance
        self.loop = loop
        self.config = config  # Configuration
        self.traffic_type = traffic_type  # Message or Bulk traffic
        self.observer = observer

        # Address_PDU and Transmission of DATA_PDUs
        self.dest_list = []  # The list of destinations of the current transmission phase */
        for i, val in enumerate(dests):
            self.dest_list.append(val["addr"])
        self.destip = dest_ip  # Destination IP address. Either unicast or multicast
        self.msid = msid  # Unique message id of the message transfer
        self.seqno = 0  # Sequence number which is incremented with each Address PDU
        self.cwnd_seqno = 0  # Incremented with each new announced transmission window
        self.cwnd = self.config[
            "initial_cwnd"
        ]  # The number of bytes of the current window
        self.tx_cwnd = self.config["initial_cwnd"]  # The current window in bytes
        self.air_datarate = self.config[
            "default_datarate"
        ]  # The minimum AirDatarate of the current transmission window
        self.tx_fragments = dict()  # The list of fragment IDs to sent in the current window { 'sent': false, 'len': xy }

        # Data Fragments
        self.fragments = fragment(message, self.config["mtu"])  # List of fragments
        self.fragments_txcount = (
            dict()
        )  # For each fragment the number of transmission is accounted
        for i, val in enumerate(self.fragments):
            self.fragments_txcount[i] = 0

        # PDU_Delay Control and Retransmission Timeout
        self.use_min_pdu_delay = (
            True  # True: All PDUs are sent PDU_Delay=0, False: PDU_Delay is used
        )
        self.pdu_delay_timer = None  # PDU Delay Timer to delay sending of messages
        self.pdu_delay = (
            MIN_PDU_DELAY  # The delay between sending Data PDUs in milliseconds
        )
        self.retry_timer = None  # Timer to trigger a Retransmission
        self.retry_timeout = 0  # The retransmission timeout in msec
        self.retry_timestamp = 0  # Timestamp at which a Retransmission shall occur
        self.tx_datarate = 1  # Tx datarate

        # Destination Context
        self.dest_status = dict()  # List of Destination status information
        for i, val in enumerate(dests):
            self.dest_status[val["addr"]] = Destination(
                self.config,
                val["addr"],
                len(self.fragments),
                val["air_datarate"],
                val["retry_timeout"],
                val["ack_timeout"],
            )

        # Statistics
        self.num_sent_data_pdus = 0  # Total number of sent PDUs
        self.start_timestamp = datetime.now()  # Start timestamp
        self.tx_phases = []  # For each transmission phase the following is accounted:
        # { tx_datarate: bit/s, air_datarate: bit/s retry_timeout: ms, fragments: []}

    def set_state(self, to):
        self.curr = to

    async def dispatch(self, event):
        func = self.states[self.curr]
        if func is not None:
            await func(event)

    def min_air_datarate(self):
        min_air_datarate = None
        for addr in self.dest_status:
            dest = self.dest_status[addr]
            if min_air_datarate is None:
                min_air_datarate = dest.air_datarate
            elif dest.air_datarate < min_air_datarate:
                min_air_datarate = dest.air_datarate
        if min_air_datarate is None:
            return pgm_cfg["mtu"]
        else:
            return min_air_datarate

    def cancel_retransmission_timer(self):
        if self.retry_timer is not None:
            self.retry_timer.cancel()
            self.retry_timer = None

    def cancel_pdu_delay_timer(self):
        if self.pdu_delay_timer is not None:
            self.pdu_delay_timer.cancel()
            self.pdu_delay_timer = None

    def increment_number_of_sent_data_pdus(self):
        for i, val in enumerate(self.dest_list):
            dest = self.dest_status[val]
            if dest is not None:
                dest.sent_data_count = dest.sent_data_count + 1

    def max_retry_timeout(self):
        max_retry_timeout = None
        for addr in self.dest_status:
            dest = self.dest_status[addr]
            if max_retry_timeout is None:
                max_retry_timeout = dest.retry_timeout
            elif dest.retry_timeout > max_retry_timeout:
                max_retry_timeout = dest.retry_timeout
        if max_retry_timeout is None:
            return self.config["max_retry_timeout"]
        else:
            return max_retry_timeout

    def get_max_retry_count(self):
        retry_count = 0
        for key in self.fragments_txcount:
            if self.fragments_txcount[key] > retry_count:
                retry_count = retry_count + 1
        return retry_count - 1

    def max_ack_timeout(self):
        max_ack_timeout = None
        for addr in self.dest_status:
            dest = self.dest_status[addr]
            if max_ack_timeout is None:
                max_ack_timeout = dest.ack_timeout
            elif dest.ack_timeout > max_ack_timeout:
                max_ack_timeout = dest.ack_timeout
        if max_ack_timeout is None:
            return self.config["max_ack_timeout"]
        else:
            return max_ack_timeout

    def retransmission_timeout(self):
        event = {"id": ClientEvents.RetransmissionTimeout}
        asyncio.ensure_future(self.dispatch(event))

    def start_retransmission_timer(self, timeout):
        if self.retry_timer is not None:
            self.cancel_retransmission_timer()
        timeout = timeout / 1000
        self.retry_timer = self.loop.call_later(timeout, self.retransmission_timeout)

    def pdu_delay_timeout(self):
        event = {"id": ClientEvents.PduDelayTimeout}
        asyncio.ensure_future(self.dispatch(event))

    def start_pdu_delay_timer(self, timeout):
        if self.pdu_delay_timer is not None:
            self.cancel_pdu_delay_timer()
        timeout = timeout / 1000
        self.pdu_delay_timer = self.loop.call_later(timeout, self.pdu_delay_timeout)

    def init_tx_phase(self, timeout_occured=False):
        _retry_timeout = 0
        _ack_timeout = 0
        _air_datarate = 0
        _cwnd = self.config["initial_cwnd"]
        remaining_bytes = 0

        ## Loss Detection for each Destination
        for addr in self.dest_status:
            dest = self.dest_status[addr]
            dest.update_missed_data_cnt()
            dest.missing_fragments = []

        ## Address_PDU Initialization
        # Initialize list of destinations. All destinations which haven´t acked
        # all fragments will be part of the destination list */
        self.dest_list = []
        for addr in self.dest_status:  # Iterate through all destinations */
            dest = self.dest_status[addr]
            dest.completed = dest.is_completed()
            if dest.completed:
                dest.ack_received = True
            else:
                dest.ack_received = False
                self.dest_list.append(addr)

        # Increment the sequence number of the transmission window. Receiver should
        # detect if we have started an new transmission phase */
        self.cwnd_seqno = self.cwnd_seqno + 1

        ## TxDatarate Control && Window Management
        # Update the txDatarate based on the measured AirDatarate
        self.tx_datarate = self.min_air_datarate()
        logging.debug("TX-CTX: Measured AirDatarate: {}".format(self.tx_datarate))

        # Calculate the current window based on the calculated TX Datarate */
        _cwnd = get_cwnd(self.config["cwnds"], self.tx_datarate, DEFAULT_AIR_DATARATE)
        if self.use_min_pdu_delay or timeout_occured:
            _cwnd = 5000  # When sending with the minPduDelay we use the default window size */

        # Calculate the number of fragments in the current window based on the cwnd */
        self.tx_fragments = unacked_fragments(self.dest_status, self.fragments, _cwnd)
        # Update the number of transmission for each fragment */
        for seq in self.tx_fragments:
            self.fragments_txcount[seq] = self.fragments_txcount[seq] + 1

        # In case of no retransmission the txDatarate is increased by the
        # configured number of inflight bytes. */
        old_tx_datarate = self.min_air_datarate()
        remaining = len(self.tx_fragments) * self.config["mtu"]
        if timeout_occured is False and remaining > 0:
            duration = round((remaining * 8 * 1000) / self.tx_datarate)
            # The txDatarate is increased by the configured number of inflightBytes.
            # This shall ensure that the txDatarate gets higher if more AirDatarate
            # is available. The limitation to the maximum number of inflightBytes
            # shall prevent packet loss due buffer overflow if the AirDatarate didn´t increase */
            self.tx_datarate = round(
                ((remaining + self.config["inflight_bytes"]) * 8 * 1000) / duration
            )
            # Limit the increase to an upper boundary */
            logging.debug("TX-CTX: Increased txDatarate: {}".format(self.tx_datarate))
            logging.debug(
                "TX-CTX: Limit TxDatarate to {}".format(
                    old_tx_datarate * (1 + self.config["max_increase"])
                )
            )
            self.tx_datarate = min(
                self.tx_datarate, old_tx_datarate * (1 + self.config["max_increase"])
            )
            logging.debug("TX-CTX: Increased txDatarate to {}".format(self.tx_datarate))

        # At full speed the txDatarate is set to the maximum configured txDatarate */
        if self.use_min_pdu_delay is True:
            self.tx_datarate = self.config["max_datarate"]
            self.use_min_pdu_delay = False
        # Correct Datarate to boundaries. Just to be sure we didn´t increase or decrease too much */
        self.tx_datarate = max(self.tx_datarate, self.config["min_datarate"])
        self.tx_datarate = min(self.tx_datarate, self.config["max_datarate"])

        ## PDU_Delay Control
        # Update PDU_Delay Timeout based on the current txDatarate */
        self.pdu_delay = round((1000 * self.config["mtu"] * 8) / self.tx_datarate)
        self.pdu_delay = max(self.pdu_delay, MIN_PDU_DELAY)

        ## Retransmission Timeout
        if self.traffic_type == Traffic.Message:
            # When sending a single message the minimum PDU_Delay can be used */
            self.pdu_delay = MIN_PDU_DELAY

            # The Retransmission Timeout is based on the maximum Timeout which was
            # previously measured for one of the destination nodes */
            _retry_timeout = self.max_retry_timeout()
            _retry_timeout = min(_retry_timeout, self.config["max_retry_timeout"])
            _retry_timeout = max(_retry_timeout, self.config["min_retry_timeout"])

            max_retry_count = self.get_max_retry_count()
            for i in range(0, max_retry_count):
                _retry_timeout = _retry_timeout * 2
            if 0 == max_retry_count:
                _retry_timeout = _retry_timeout * (1 + self.config["max_increase"])
            logging.debug(
                "TX-CTX: Message: Set RetryTimeout to {} at retrycount of {}".format(
                    _retry_timeout, max_retry_count
                )
            )

            # Retry Timeout
            self.retry_timestamp = datetime.now() + timedelta(
                milliseconds=_retry_timeout
            )
            self.retry_timeout = _retry_timeout
            self.air_datarate = 0  # not used
        else:  # Bulk Traffic
            # The retransmission timeout is based on an estimation of the airDatarate.
            # This estimation takes into account that the airDatarate can have a large
            # decrease. This amount of decrease can be configure. e.g. in a worst case
            # scenario the datarate can be 50% smaller due modulation change of the waveform. */
            _air_datarate = round(
                self.min_air_datarate() * (1 - self.config["max_decrease"])
            )
            # Calculate the retransmission timeout based on the remaining bytes to sent
            remaining_bytes = len(self.tx_fragments) * self.config["mtu"]
            # The Retransmission Timeout takes into account that extra Address PDUs have to be sent
            remaining_bytes = remaining_bytes + self.config["mtu"]  # ExtraAddressPdu
            # Calculate the timeInterval it takes to sent the data to the receiver
            _retry_timeout = round((remaining_bytes * 8 * 1000) / _air_datarate)
            # At multicast communication the retransmission timeout must take into account that
            # multiple time-sliced ACK_PDUs have to be sent.
            if len(self.dest_list) > 1:
                _retry_timeout = (
                    _retry_timeout + len(self.dest_list) * ACK_PDU_DELAY_MSEC
                )
            # The AckTimeout is the maximum value of one of the destinations
            _ack_timeout = self.max_ack_timeout()

            # Limit the RetransmissionTimeout
            _retry_timeout = max(_retry_timeout, self.config["min_retry_timeout"])
            _retry_timeout = min(_retry_timeout, self.config["max_retry_timeout"])
            # Calculate the timestamp at which a retransmission has too occur
            logging.debug(
                "TX-CTX Bulk: Set RetryTimeout {} to for AirDatarate of {} and ackTimeout of {}".format(
                    _retry_timeout, _air_datarate, _ack_timeout
                )
            )
            # Retry Timeout
            self.retry_timestamp = datetime.now() + timedelta(
                milliseconds=_retry_timeout
            )
            self.retry_timeout = _retry_timeout
            logging.debug(
                "TX Init() tx_datarate: {} air_datarate: {} retry_timeout: {} ack_timeout: {}".format(
                    self.tx_datarate, _air_datarate, _retry_timeout, _ack_timeout
                )
            )

        ## Update Statistics
        fragment_list = []
        for key in self.tx_fragments:
            fragment_list.append(key)
        dest_list = []
        for i, val in enumerate(self.dest_list):
            dest_list.append(val)

        tx_phase = dict()
        tx_phase["dest_list"] = dest_list
        tx_phase["tx_datarate"] = self.tx_datarate
        tx_phase["air_datarate"] = _air_datarate
        tx_phase["retry_timeout"] = _retry_timeout
        tx_phase["fragment_list"] = fragment_list
        self.tx_phases.append(tx_phase)

        logging.debug(
            "TX +--------------------------------------------------------------+"
        )
        logging.debug(
            "SND | TX Phase                                                     |"
        )
        logging.debug(
            "SND |--------------------------------------------------------------+"
        )
        logging.debug("SND | dest_list: {}".format(dest_list))
        logging.debug("SND | cwnd: {}".format(_cwnd))
        logging.debug("SND | seqnohi: {}".format(get_seqnohi(self.tx_fragments)))
        logging.debug("SND | tx_fragments: {}".format(self.tx_fragments))
        ack_recv_status = dict()
        for addr in self.dest_status:
            if self.dest_status[addr].completed is False:
                ack_recv_status[addr] = self.dest_status[addr].ack_received
        logging.debug("SND | AckRecvStatus: {}".format(ack_recv_status))
        logging.debug("SND | OldTxDatarate: {}".format(old_tx_datarate))
        logging.debug("SND | NewTxDatarate: {}".format(self.tx_datarate))
        logging.debug("SND | AirDatarate: {}".format(_air_datarate))
        logging.debug("SND | AckTimeout: {}".format(_ack_timeout))
        logging.debug("SND | RetryTimeout: {}".format(_retry_timeout))
        logging.debug("SND | InflightBytes: {}".format(self.config["inflight_bytes"]))
        logging.debug("SND | RemainingBytes: {}".format(remaining_bytes))
        logging.debug(
            "SND | RetryCount: {} Max: {}".format(
                self.get_max_retry_count(), self.config["max_retry_count"]
            )
        )
        logging.debug(
            "TX +--------------------------------------------------------------+"
        )

    def get_delivery_status(self):
        deltatime = datetime.now() - self.start_timestamp
        delivery_time = timedelta_milli(deltatime)
        loss = round(
            100
            * ((self.num_sent_data_pdus - len(self.fragments)) / len(self.fragments))
        )
        status = dict()
        status["tx_datarate"] = self.tx_datarate
        status["goodput"] = calc_datarate(
            self.start_timestamp, message_len(self.fragments)
        )
        status["delivery_time"] = delivery_time
        status["num_data_pdus"] = len(self.fragments)
        status["num_sent_data_pdus"] = self.num_sent_data_pdus
        status["loss"] = loss
        return status

    def get_next_tx_fragment(self):
        for key in self.tx_fragments:
            if self.tx_fragments[key]["sent"] is False:
                return key
        return 0

    def is_final_fragment(self, fragid):
        last_id = -1
        for key in self.tx_fragments:
            last_id = key
        if fragid == last_id:
            return True
        return False

    def calc_air_datarate(self, remote_ipaddr, ts_ecr, missing_fragments, tvalue):
        if len(self.tx_fragments) == 0:
            return None  # Nothing was sent
        if ts_ecr is not None:
            # AddressPdu was received
            bytes = get_sent_bytes(self.tx_fragments)
            air_datarate = round(bytes * 8 * 1000 / tvalue)
            logging.debug(
                "TX: {} measured air-datarate: {} bit/s send-bytes: {} tValue: {}".format(
                    remote_ipaddr, air_datarate, bytes, tvalue
                )
            )
        else:
            # AddressPdu wasn´t received
            first_seqno = self.get_first_received_fragid(missing_fragments)
            if first_seqno is None:
                return None  # Nothing was received, neither AddressPdu nor DataPdu
            bytes = self.calc_received_bytes(first_seqno)
            air_datarate = round(bytes * 8 * 1000 / tvalue)
            logging.debug(
                "TX: {} measured air-datarate: {} bit/s 1st rx-fragment: {}end-bytes: {} tValue: {}".format(
                    remote_ipaddr, air_datarate, first_seqno, bytes, tvalue
                )
            )
        return air_datarate

    async def state_IDLE(self, ev):
        logging.debug("SND | IDLE: {}".format(ev["id"]))
        self.cancel_pdu_delay_timer()
        self.cancel_retransmission_timer()
        now = datetime.now()

        match ev["id"]:
            case ClientEvents.Start:
                self.init_tx_phase()
                # Send Address PDU
                address_pdu = AddressPdu()
                address_pdu.type = PduType.Address
                address_pdu.total = len(self.fragments)
                address_pdu.cwnd = len(self.tx_fragments)
                address_pdu.seqnohi = get_seqnohi(self.tx_fragments)
                address_pdu.src_ipaddr = self.cli.src_ipaddr
                address_pdu.msid = self.msid
                address_pdu.tsval = date_to_milli(now)
                for i, val in enumerate(self.dest_list):
                    dest_entry = DestinationEntry()
                    dest_entry.dest_ipaddr = val
                    dest_entry.seqno = self.seqno
                    address_pdu.dst_entries.append(dest_entry)
                self.seqno += 1
                if self.traffic_type == Traffic.Message:
                    # Append Data if we have only a small message to transfer */
                    address_pdu.payload = self.fragments[0]
                    self.tx_fragments[0]["sent"] = True
                    self.tx_fragments[0]["len"] = len(self.fragments[0])
                    self.increment_number_of_sent_data_pdus()
                    self.num_sent_data_pdus += 1
                address_pdu.log("SND")
                pdu = address_pdu.to_buffer()
                self.cli.sendto(pdu, self.destip)

                if self.traffic_type == Traffic.Message:
                    # Start Retransmission timer
                    timeout = round(timedelta_milli(self.retry_timestamp - now))
                    logging.debug(
                        "SND | start Retransmission timer with {} msec delay".format(
                            timeout
                        )
                    )
                    self.cancel_retransmission_timer()
                    self.start_retransmission_timer(timeout)
                    # Change State to WAITING_FOR_ACKS
                    logging.debug("SND | change state to WAITING_FOR_ACKS")
                    self.set_state(ClientStates.WaitingForAcks)
                else:  # BULK traffoc mpde
                    # Start PDU_Delay Timer
                    logging.debug(
                        "SND | IDLE - start PDU Delay timer with a {} msec timeout".format(
                            MIN_PDU_DELAY
                        )
                    )
                    self.start_pdu_delay_timer(MIN_PDU_DELAY)
                    logging.debug("SND | IDLE - Change state to SENDING_DATA")
                    self.set_state(ClientStates.SendingData)

            case _:
                pass

    async def state_sending_data(self, ev):
        logging.debug("SND | SENDING_DATA: {}".format(ev["id"]))

        match ev["id"]:
            case ClientEvents.PduDelayTimeout:
                self.cancel_pdu_delay_timer()

                # Send Data PDU
                data_pdu = DataPdu()
                data_pdu.cwnd = len(self.tx_fragments)
                data_pdu.seqnohi = get_seqnohi(self.tx_fragments)
                data_pdu.src_ipaddr = self.cli.src_ipaddr
                data_pdu.msid = self.msid
                data_pdu.seqno = self.get_next_tx_fragment()
                data_pdu.cwnd_seqno = self.cwnd_seqno
                data_pdu.data.extend(self.fragments[data_pdu.seqno])
                data_pdu.log("SND")

                pdu = data_pdu.to_buffer()
                logging.debug(
                    "SND | SND DataPdu[{}] len: {} cwnd_seqno: {}".format(
                        data_pdu.seqno, len(data_pdu.data), data_pdu.cwnd_seqno
                    )
                )
                self.cli.sendto(pdu, self.destip)
                self.tx_fragments[data_pdu.seqno]["sent"] = True
                self.tx_fragments[data_pdu.seqno]["len"] = len(data_pdu.data)
                self.increment_number_of_sent_data_pdus()
                self.num_sent_data_pdus += 1

                logging.debug(
                    "SND | SENDING_DATA - start PDU Delay timer with a {} msec timeout".format(
                        self.pdu_delay
                    )
                )
                self.start_pdu_delay_timer(self.pdu_delay)

                if self.is_final_fragment(data_pdu.seqno):
                    logging.debug(
                        "SND | SENDING_DATA - Change state to SENDING_EXTRA_ADDRESS_PDU"
                    )
                    self.set_state(ClientStates.SendingExtraAddressPdu)

            case _:
                pass

    async def state_sending_extra_address_pdu(self, ev):
        logging.debug("SND | SENDING_EXTRA_ADDRESS_PDU: {}".format(ev["id"]))

        match ev["id"]:
            case ClientEvents.PduDelayTimeout:
                self.cancel_pdu_delay_timer()
                now = datetime.now()

                # Send Address PDU
                address_pdu = AddressPdu()
                address_pdu.type = PduType.ExtraAddress
                address_pdu.total = len(self.fragments)
                address_pdu.cwnd = len(self.tx_fragments)
                address_pdu.seqnohi = get_seqnohi(self.tx_fragments)
                address_pdu.src_ipaddr = self.cli.src_ipaddr
                address_pdu.msid = self.msid
                address_pdu.tsval = date_to_milli(now)
                for i, val in enumerate(self.dest_list):
                    dest_entry = DestinationEntry()
                    dest_entry.dest_ipaddr = val
                    dest_entry.seqno = self.seqno
                    address_pdu.dst_entries.append(dest_entry)
                self.seqno += 1

                if self.traffic_type == Traffic.Message:
                    # Append Data if we have only a small message to transfer */
                    address_pdu.payload = self.fragments[0]
                    self.tx_fragments[0]["sent"] = True
                    self.tx_fragments[0]["len"] = len(address_pdu.payload)
                    self.increment_number_of_sent_data_pdus()
                    self.num_sent_data_pdus += 1
                address_pdu.log("SND")
                pdu = address_pdu.to_buffer()
                self.cli.sendto(pdu, self.destip)

                # Start Retransmission timer
                timeout = round(timedelta_milli(self.retry_timestamp - now))
                logging.debug(
                    "SND | start Retransmission timer with {} msec delay".format(
                        timeout
                    )
                )
                self.cancel_retransmission_timer()
                self.start_retransmission_timer(timeout)
                # Change State to WAITING_FOR_ACKS
                logging.debug("SND | change state to WAITING_FOR_ACKS")
                self.set_state(ClientStates.WaitingForAcks)

            case ClientEvents.RetransmissionTimeout:
                self.cancel_retransmission_timer()

            case _:
                pass

    async def state_WAITING_FOR_ACKS(self, ev):
        logging.debug("SND | WAITING_FOR_ACKS: {}".format(ev["id"]))

        match ev["id"]:
            case ClientEvents.AckPdu:
                remote_ipaddr = ev["remote_ipaddr"]
                info_entry = ev["info_entry"]
                tvalue = info_entry.tvalue

                # Check if ACK sender is in our destination list
                if remote_ipaddr not in self.dest_list:
                    logging.debug("TX: Ignore ACK from {}".format(remote_ipaddr))
                    return
                if remote_ipaddr not in self.dest_status:
                    logging.debug("TX: Ignore ACK from {}".format(remote_ipaddr))
                    return
                dest_status = self.dest_status[remote_ipaddr]

                # Update the Ack-Status of each fragment
                dest_status.update_fragment_ack_status(
                    info_entry.missing_seqnos, info_entry.seqnohi
                )
                # If this AckPdu with the same timestamp hasn´t already been received
                # before we update the retryTimeout and the ackTimeout */
                if dest_status.is_duplicate(info_entry.tsecr) is False:
                    # Update RetryTimeout for that destination
                    dest_status.update_retry_timeout(info_entry.tsecr)
                    # Update the AckTimeout for that destination
                    dest_status.update_ack_timeout(info_entry.tsecr, tvalue)
                # We have received an Ack from that remote address
                dest_status.ack_received = True

                # Update the airDatarate based on the received tvalue
                if tvalue > 0:
                    air_datarate = self.calc_air_datarate(
                        remote_ipaddr,
                        info_entry.tsecr,
                        info_entry.missing_seqnos,
                        tvalue,
                    )
                    logging.debug(
                        "TX Measured air-datarate: {} and tvalue: {} sec".format(
                            air_datarate, tvalue / 1000
                        )
                    )
                    # Prevent increase of airDatarate in a wrong case
                    if air_datarate > self.tx_datarate:
                        logging.debug(
                            "SND | air-datarate {} is larger than tx-datarate - Update air-datarate to {}".format(
                                air_datarate, self.tx_datarate
                            )
                        )
                        dest_status.update_air_datarate(self.tx_datarate)
                    else:
                        dest_status.update_air_datarate(air_datarate)
                dest_status.log()

                # If we have received all ACK PDU we can continue with the next
                # transmission phase sending Data_PDU retransmissions. If ACK_PDUs
                # still are missing we continue waiting */
                if received_all_acks(self.dest_status):
                    self.cancel_retransmission_timer()
                    self.init_tx_phase()

                    if self.get_max_retry_count() > self.config["max_retry_count"]:
                        logging.debug("SND | Max Retransmission Count Reached")
                        logging.debug("SND | change state to Idle")
                        self.set_state(ClientStates.Idle)

                        num_acked_dests = 0
                        ack_status = dict()
                        for addr in self.dest_status:
                            dest = self.dest_status[addr]
                            if dest.completed is True:
                                num_acked_dests += 1
                            ack_status[addr] = dict()
                            ack_status[addr]["ack_status"] = dest.completed
                            ack_status[addr]["ack_timeout"] = dest.ack_timeout
                            ack_status[addr]["retry_timeout"] = dest.retry_timeout
                            ack_status[addr]["retry_count"] = self.get_max_retry_count()
                            ack_status[addr]["num_sent"] = dest.sent_data_count
                            ack_status[addr]["air_datarate"] = dest.air_datarate
                            ack_status[addr]["missed"] = dest.missed_data_count

                        self.cli.transmission_finished(
                            self.msid, self.get_delivery_status(), ack_status
                        )
                        return

                    # Send Address PDU
                    address_pdu = AddressPdu()
                    address_pdu.type = PduType.Address
                    address_pdu.total = len(self.fragments)
                    address_pdu.cwnd = len(self.tx_fragments)
                    address_pdu.seqnohi = get_seqnohi(self.tx_fragments)
                    address_pdu.src_ipaddr = self.cli.src_ipaddr
                    address_pdu.msid = self.msid
                    address_pdu.tsval = date_to_milli(datetime.now())
                    for i, val in enumerate(self.dest_list):
                        dest_entry = DestinationEntry()
                        dest_entry.dest_ipaddr = val
                        dest_entry.seqno = self.seqno
                        address_pdu.dst_entries.append(dest_entry)
                    self.seqno += 1

                    if self.traffic_type == Traffic.Message and len(self.dest_list) > 0:
                        # Append Data if we have only a small message to transfer
                        address_pdu.payload = self.fragments[0]
                        self.tx_fragments[0] = dict()
                        self.tx_fragments[0]["len"] = len(self.fragments[0])
                        self.tx_fragments[0]["sent"] = True
                        self.increment_number_of_sent_data_pdus()
                        self.num_sent_data_pdus += 1

                    address_pdu.log("SND")
                    pdu = address_pdu.to_buffer()
                    self.cli.sendto(pdu, self.destip)
                    if len(self.dest_list) > 0:
                        if self.traffic_type == Traffic.Message:
                            # Start Retransmission timer
                            timeout = round(
                                timedelta_milli(self.retry_timestamp - datetime.now())
                            )
                            logging.debug(
                                "SND | start Retransmission timer with {} msec delay".format(
                                    timeout
                                )
                            )
                            self.cancel_retransmission_timer()
                            self.start_retransmission_timer(timeout)
                            # Change State to WAITING_FOR_ACKS
                            logging.debug("SND | change state to WAITING_FOR_ACKS")
                            self.set_state(ClientStates.WaitingForAcks)
                        else:  # BULK traffoc mpde
                            # Start PDU_Delay Timer
                            logging.debug(
                                "SND | IDLE - start PDU Delay timer with a {} msec timeout".format(
                                    MIN_PDU_DELAY
                                )
                            )
                            self.start_pdu_delay_timer(MIN_PDU_DELAY)
                            logging.debug("SND | IDLE - Change state to SENDING_DATA")
                            self.set_state(ClientStates.SendingData)
                    else:
                        # Change state to FINISHED
                        self.cancel_retransmission_timer()
                        logging.debug("SND | change state to FINISHED")
                        self.set_state(ClientStates.Finished)

                        ack_status = dict()
                        for addr in self.dest_status:
                            dest = self.dest_status[addr]
                            ack_status[addr] = dict()
                            ack_status[addr]["ack_status"] = dest.completed
                            ack_status[addr]["ack_timeout"] = dest.ack_timeout
                            ack_status[addr]["retry_timeout"] = dest.retry_timeout
                            ack_status[addr]["retry_count"] = self.get_max_retry_count()
                            ack_status[addr]["num_sent"] = dest.sent_data_count
                            ack_status[addr]["air_datarate"] = dest.air_datarate
                            ack_status[addr]["missed"] = dest.missed_data_count

                        self.cli.transmission_finished(
                            self.msid, self.get_delivery_status(), ack_status
                        )

            case ClientEvents.RetransmissionTimeout:
                self.cancel_retransmission_timer()
                num_acked_dests = 0
                ack_status = dict()
                remove_list = []

                # Update MissedAckCount, Retry Count and AirDatarate for all destinations we have missed the AckPdu */
                for addr in self.dest_status:
                    dest_status = self.dest_status[addr]
                    if dest_status.ack_received is False:
                        # Reduce air_datarate
                        dest_status.update_air_datarate_after_timeout()
                        # Increment the missed ACK counter
                        dest_status.missed_ack_count += 1
                        logging.debug(
                            "SND | {} missed acks: {}".format(
                                addr, dest_status.missed_ack_count
                            )
                        )

                        # Add a list of missing data pdus for calculating the missed_data_count
                        missing = []
                        for i, val in enumerate(self.tx_fragments):
                            missing.append(i)
                        dest_status.missing_fragments = missing

                        # Check if destination has reached the maximum missed count
                        # If so, remove it from the list of active destinations
                        if (
                            dest_status.missed_ack_count
                            >= self.config["max_missed_acks"]
                        ):
                            remove_list.append(addr)
                            self.use_min_pdu_delay = True

                    # Ack status
                    if dest_status.completed is True:
                        num_acked_dests += 1
                    status = dict()
                    status["ack_status"] = dest_status.completed
                    status["ack_timeout"] = dest_status.ack_timeout
                    status["retry_timeout"] = dest_status.retry_timeout
                    status["retry_count"] = self.get_max_retry_count()
                    status["num_sent"] = dest_status.sent_data_count
                    status["air_datarate"] = dest_status.air_datarate
                    status["missed"] = dest_status.missed_data_count
                    ack_status[addr] = status

                # Remove destinations if they have not responded several times
                for i, val in enumerate(remove_list):
                    logging.debug(
                        "SND | Remove {} from active destinations".format(val)
                    )
                    del self.dest_status[val]

                # Check if transmission should be canceled
                if self.get_max_retry_count() > self.config["max_retry_count"]:
                    logging.debug("SND | Max Retransmission Count Reached")
                    logging.debug("SND | change state to Idle")
                    self.set_state(ClientStates.Idle)

                    if num_acked_dests > 0:
                        self.cli.transmission_finished(
                            self.msid, self.get_delivery_status(), ack_status
                        )
                    else:
                        self.cli.transmission_finished(
                            self.msid, self.get_delivery_status(), ack_status
                        )
                    return

                # Initialize after Retransmission
                logging.debug(
                    "TX WAITING_FOR_ACKS - initTransmissionPhase after RetransmissionTimeout"
                )
                self.init_tx_phase(timeout_occured=True)

                # Send Address PDU
                address_pdu = AddressPdu()
                address_pdu.type = PduType.Address
                address_pdu.total = len(self.fragments)
                address_pdu.cwnd = len(self.tx_fragments)
                address_pdu.seqnohi = get_seqnohi(self.tx_fragments)
                address_pdu.src_ipaddr = self.cli.src_ipaddr
                address_pdu.msid = self.msid
                address_pdu.tsval = date_to_milli(datetime.now())
                for i, val in enumerate(self.dest_list):
                    dest_entry = DestinationEntry()
                    dest_entry.dest_ipaddr = val
                    dest_entry.seqno = self.seqno
                    address_pdu.dst_entries.append(dest_entry)
                self.seqno += 1

                if self.traffic_type == Traffic.Message:
                    # Append Data if we have only a small message to transfer */
                    address_pdu.payload = self.fragments[0]
                    self.tx_fragments[0] = dict()
                    self.tx_fragments[0]["sent"] = True
                    self.tx_fragments[0]["len"] = len(self.fragments[0])
                    self.increment_number_of_sent_data_pdus()
                    self.num_sent_data_pdus += 1
                address_pdu.log("SND")
                pdu = address_pdu.to_buffer()
                self.cli.sendto(pdu, self.destip)

                if len(self.dest_list) > 0:
                    if self.traffic_type == Traffic.Message:
                        # Start Retransmission timer
                        timeout = round(
                            timedelta_milli(self.retry_timestamp - datetime.now())
                        )
                        logging.debug(
                            "SND | start Retransmission timer with {} msec delay".format(
                                timeout
                            )
                        )
                        self.cancel_retransmission_timer()
                        self.start_retransmission_timer(timeout)
                    else:
                        # Start PDU Delay Timer
                        logging.debug(
                            "SND | start PDU Delay timer with a {} msec timeout".format(
                                MIN_PDU_DELAY
                            )
                        )
                        self.start_pdu_delay_timer(MIN_PDU_DELAY)
                        # Change state to SENDING_DATA */
                        logging.debug("SND | change state to SENDING_DATA")
                        self.set_state(ClientStates.SendingData)
                else:
                    # Change state to FINISHED */
                    logging.debug("SND | WAITING_FOR_ACKS - Change state to FINISHED")
                    self.set_state(ClientStates.Finished)
                    self.cli.transmission_finished(
                        self.msid, self.get_delivery_status(), ack_status
                    )
            case _:
                pass

    async def state_finished(self, ev):
        logging.debug("SND | WAITING_FOR_ACKS: {}".format(ev["id"]))

    def log(self, state):
        logging.debug(
            "{} +--------------------------------------------------------------+".format(
                state
            )
        )
        logging.debug(
            "{} | Client State                                                 |".format(
                state
            )
        )
        logging.debug(
            "{} +--------------------------------------------------------------+".format(
                state
            )
        )
        logging.debug("{} | dest_list: {}".format(state, self.dest_list))
        logging.debug("{} | destip: {}".format(state, self.destip))
        logging.debug("{} | msid: {}".format(state, self.msid))
        logging.debug("{} | traffic_type: {}".format(state, self.traffic_type))
        logging.debug("{} | PduDelay: {} msec".format(state, self.pdu_delay))
        logging.debug("{} | Datarate: {} bit/s".format(state, self.tx_datarate))
        logging.debug(
            "{} | MinDatarate: {} bit/s".format(state, self.config["min_datarate"])
        )
        logging.debug(
            "{} | MaxDatarate: {} bit/s".format(state, self.config["max_datarate"])
        )
        logging.debug(
            "{} | MaxIncreasePercent: {} %".format(
                state, self.config["max_increase"] * 100
            )
        )
        for key in self.dest_status:
            logging.debug(
                "{} | fragmentsAckStatus[{}]: {}".format(
                    state, key, self.dest_status[key].fragment_ack_status
                )
            )
        logging.debug(
            "{} +--------------------------------------------------------------+".format(
                state
            )
        )


# client
class Client(asyncio.DatagramProtocol):
    def __init__(self, loop, config, observer):
        self.loop = loop
        self.src_ipaddr = config["src_ipaddr"]
        self.mcast_ipaddr = config["mcast_ipaddr"]
        self.mcast_ttl = config["mcast_ttl"]
        self.dport = config["dport"]
        self.aport = config["aport"]
        self.tx_ctx_list = dict()
        self.transport = None
        self.iface_index = config["iface_index"]
        self.observer = observer

        # client sends multicast address and data in dport and listens
        # for unicast ack on aport
        self.sock = socket.socket(family=FAMILY, type=socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.mcast_ttl)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, False)
        set_multicast_if(
            self.sock, self.iface_index, self.mcast_ipaddr, self.src_ipaddr
        )
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if is_windows:
            self.sock.bind((self.src_ipaddr, self.aport))
            mreq_bytes = struct.pack(
                "!4sI", socket.inet_aton(self.mcast_ipaddr), self.iface_index
            )
            self.sock.setsockopt(
                socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq_bytes
            )
        else:
            self.sock.bind((self.src_ipaddr, self.aport))
            self.sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                make_ip_mreqn_struct(
                    self.mcast_ipaddr, self.iface_index, self.src_ipaddr
                ),
            )

        logging.info(f"socket listening for ack on port: {self.aport}")

        self.task = asyncio.ensure_future(self.start())

    async def start(self):
        coro = self.loop.create_datagram_endpoint(lambda: self, sock=self.sock)
        await asyncio.wait_for(coro, 2)

    def generate_msid(self):
        for i in range(0, 1000):
            msid = random.randrange(10000000)
            if msid not in self.tx_ctx_list:
                return msid
        return -1

    async def send_message(self, message, dest_ips, node_info):
        await self.init_send(message, dest_ips, Traffic.Message, node_info)

    async def send_bulk(self, message, dest_ips, node_info):
        await self.init_send(message, dest_ips, Traffic.Bulk, node_info)

    async def init_send(self, message, dest_ips, traffic_type, node_info):
        # # if there is only one destionation, this is considered to be a
        # # unicast communication else this is multicast and message is sent
        # # to group ip
        # if len(dest_ips) == 1:
        #     dest_ip = dest_ips[0]
        # else:
        dest_ip = self.mcast_ipaddr

        # generate unique message-id
        msid = self.generate_msid()
        if msid == -1:
            logging.error("send_message() FAILED with: No msid found")
            return False

        # convert IPv4 addresses from string format into 32bit values
        dests = []
        for val in dest_ips:
            entry = dict()
            entry["addr"] = val
            if val in node_info:
                node_info = node_info[val]
                entry["air_datarate"] = node_info["air_datarate"]
                entry["ack_timeout"] = node_info["ack_timeout"]
                entry["retry_timeout"] = node_info["retry_timeout"]
            else:
                entry["air_datarate"] = DEFAULT_AIR_DATARATE
                entry["ack_timeout"] = DEFAULT_ACK_TIMEOUT
                entry["retry_timeout"] = DEFAULT_RETRY_TIMEOUT
            dests.append(entry)

        state = ClientState(
            self,
            self.loop,
            pgm_cfg,
            dest_ip,
            dests,
            msid,
            message,
            traffic_type,
            self.observer,
        )

        state.log("SEND")

        self.tx_ctx_list[msid] = state
        event = {"id": ClientEvents.Start}
        await state.dispatch(event)

    def sendto(self, message, dest):
        logging.debug("send packet to addr {} port {}".format(dest, self.dport))
        if self.transport is not None:
            self.transport.sendto(message, (dest, self.dport))
        else:
            logging.error("UDP socket is not ready")

    def connection_made(self, transport):
        self.transport = transport
        logging.debug("UDP socket is ready")

    def on_ack_pdu(self, data):
        ack_pdu = AckPdu()
        ack_pdu.from_buffer(data)
        ack_pdu.log("SND")
        remote_ipaddr = ack_pdu.src_ipaddr

        for i, info_entry in enumerate(ack_pdu.info_entries):
            if info_entry.remote_ipaddr != self.src_ipaddr:
                continue  # Ack_InfoEntry is not addressed to me
            msid = info_entry.msid
            if msid not in self.tx_ctx_list:
                continue  # Received AckInfoEntry for unkown MessageId
            ctx = self.tx_ctx_list[msid]
            ev = dict()
            ev["id"] = ClientEvents.AckPdu
            ev["remote_ipaddr"] = remote_ipaddr
            ev["info_entry"] = info_entry
            asyncio.ensure_future(ctx.dispatch(ev))

    def datagram_received(self, data, addr):
        pdu = Pdu()
        pdu.from_buffer(data[:MINIMUM_PACKET_LEN])
        pdu.log("SND")

        logging.debug(
            "SND | Received packet from {} type: {} len:{}".format(
                addr, pdu.type, pdu.len
            )
        )

        if pdu.type == int(PduType.Ack):
            self.on_ack_pdu(data)
        else:
            logging.debug("Received unkown PDU type {}".format(pdu.type))

    def transmission_finished(self, msid, delivery_status, ack_status):
        if msid not in self.tx_ctx_list:
            return
        ctx = self.tx_ctx_list[msid]

        ctx.observer.transmission_finished(msid, delivery_status, ack_status)
        if ctx.future is not None:
            ctx.future.set_result((delivery_status, ack_status))

    def error_received(self, err):
        logging.error("Error received:", err)

    def connection_lost():
        logging.debug("Socket closed, stop the event loop")
