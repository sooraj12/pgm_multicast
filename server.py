import asyncio

from pgm_multicast.server import listen


if __name__ == "__main__":
    asyncio.run(listen())
