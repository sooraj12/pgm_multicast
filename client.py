import asyncio
from hypercorn.config import Config
from hypercorn.asyncio import serve

from quart import Quart, request
from logging.config import dictConfig

from pgm_multicast.client import ClientProtocol, ClientTransport
from pgm_multicast.config import transport_conf


def setup_logging():
    return dictConfig(
        {
            "version": 1,
            "formatters": {
                "default": {
                    "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
                }
            },
            "handlers": {
                "wsgi": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                }
            },
            "root": {"level": "DEBUG", "handlers": ["wsgi"]},
        }
    )


setup_logging()

app = Quart(__name__)
app.config.from_mapping(
    SECRET_KEY="dev",
)
config = Config()
config.bind = ["0.0.0.0:9001"]
config.debug = False

client_protocol = None

@app.before_serving
async def setup():
    global client_protocol
    protocol = ClientProtocol()
    event_loop = asyncio.get_running_loop()
    transport = ClientTransport(event_loop, protocol, transport_conf)
    protocol.init_connection(transport)
    await asyncio.sleep(2)
    client_protocol = protocol


@app.route("/health")
async def health():
    return {"message": "OK"}


@app.route("/send_mc", methods=["POST"])
async def send():
    data = await request.get_json()
    await client_protocol.send_message(data, ["192.168.1.3"])
    return {"message": "OK"}


if __name__ == "__main__":
    asyncio.run(serve(app, config))
