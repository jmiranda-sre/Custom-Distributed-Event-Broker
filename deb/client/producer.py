"""Producer client — sends messages to the broker over TCP."""

import asyncio
import logging
from typing import Optional

from deb.protocol.frame import Cmd, Frame

logger = logging.getLogger("deb.client.producer")


class Producer:
    """Async producer client."""

    def __init__(self, broker_host: str = "127.0.0.1", broker_port: int = 9090):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self.broker_host, self.broker_port
        )
        logger.info("Producer connected to %s:%d", self.broker_host, self.broker_port)

    async def close(self):
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except OSError:
                pass
        logger.info("Producer disconnected")

    async def send(self, topic: str, value: str, key: Optional[str] = None) -> int:
        """Send a message to the broker. Returns the assigned offset."""
        if not self._writer:
            raise ConnectionError("Not connected")

        frame = Frame(Cmd.PRODUCE, {"topic": topic, "key": key, "value": value})
        self._writer.write(frame.encode())
        await self._writer.drain()

        resp = await Frame.read_from(self._reader)
        if resp.cmd == Cmd.PRODUCE_ACK:
            return resp.payload["offset"]
        elif resp.cmd == Cmd.ERROR:
            raise RuntimeError(f"Broker error: {resp.payload}")
        else:
            raise RuntimeError(f"Unexpected response: {resp.cmd.name}")
