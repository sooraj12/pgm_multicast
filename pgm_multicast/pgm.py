from .constants import MIN_BULK_SIZE
from .logger import logging


# transport
class Transport:
    def __init__(self, protocol, config):
        self._protocol = protocol
        self._config = config
        self.min_bulk_size = MIN_BULK_SIZE
        self.node_info = dict()

    def transmission_finished(self, msid, delivery_status, ack_status):
        for addr, val in ack_status.items():
            if addr not in self.node_info:
                self.node_info[addr] = dict()
                self.node_info[addr]["air_datarate"] = val["air_datarate"]
                self.node_info[addr]["retry_timeout"] = val["retry_timeout"]
                self.node_info[addr]["ack_timeout"] = val["ack_timeout"]
        self._protocol.delivery_completed(msid, delivery_status, ack_status)


# protocol
class Protocol:
    def init_connection(self, transport):
        self.transport = transport
        logging.debug("P_MUL protocol is ready")

    def data_received(self, data, addr):
        logging.debug("Received a data from {}".format(addr))

    def delivery_completed(self, msid, delivery_status, ack_status):
        logging.debug(
            "Delivery of Message-ID {} finished with {} {}".format(
                msid, delivery_status, ack_status
            )
        )
        
        # write to file
        with open('logg.txt', 'a') as fl:
            fl.writelines( "Delivery of Message-ID {} finished with {} {}\n".format(
                msid, delivery_status, ack_status
            ))
