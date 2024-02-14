import msgpack
import gzip
import sys

from .pgm import Transport, Protocol
from .pgm_client import Client
from .logger import logging


class ClientTransport(Transport):
    def __init__(self, loop, protocol, config):
        super().__init__(protocol, config)
        self._client = Client(loop, self._config, self)

    async def sendto(self, message, dest_ips):
        # serialize
        serialized = msgpack.packb(message)
        logging.debug(
            f"SND | sending message of len {sys.getsizeof(serialized)} before compressing"
        )
        # compress
        compressed_data = gzip.compress(serialized)
        logging.debug(
            f"SND | sending message of len {sys.getsizeof(compressed_data)} after compressing"
        )
        if len(compressed_data) < self.min_bulk_size:
            await self._client.send_message(compressed_data, dest_ips, self.node_info)
        else:
            await self._client.send_bulk(compressed_data, dest_ips, self.node_info)


class ClientProtocol(Protocol):
    async def send_message(self, message, dest_ips):
        logging.debug(
            f"SND | sending message of len {sys.getsizeof(message)} before encoding"
        )
        await self.transport.sendto(message, dest_ips)
