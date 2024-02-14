import asyncio

import gzip
import msgpack
import sys
import requests

from io import BytesIO
from .logger import logging
from .pgm import Transport, Protocol
from .pgm_server import Server
from .config import transport_conf


class ServerTransport(Transport):
    def __init__(self, loop, protocol, config):
        super().__init__(protocol, config)
        self._server = Server(loop, self._config, self)

    def message_received(self, message):
        logging.debug(
            f"RCV | Received message of len {sys.getsizeof(message)} before decompressing"
        )
        buffer = BytesIO(message)
        with gzip.GzipFile(fileobj=buffer) as f:
            decompressed_content = f.read()
            logging.debug(
                f"RCV | Received message of len {sys.getsizeof(decompressed_content)} after decompressing"
            )
            data = msgpack.unpackb(decompressed_content, raw=False)
        self._protocol.message_received(data)


class ServerProtocol(Protocol):
    def message_received(self, message):
        logging.info(
            f"new message of length {sys.getsizeof(message)} received, send to application"
        )
        url = "http://localhost:5000/webhook"
        requests.post(url, json=message)


async def forever():
    while True:
        try:
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            pass


async def setup_server():
    loop = asyncio.get_running_loop()
    protocol = ServerProtocol()
    transport = ServerTransport(loop, protocol, transport_conf)
    protocol.init_connection(transport)
    await asyncio.sleep(2)


async def listen():
    await setup_server()
    await forever()
