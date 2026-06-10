"""Consumer client — receives messages from broker with at-least-once guarantee.

Flow:
  1. SUBSCRIBE → broker returns committed offset for (topic, group)
  2. FETCH(offset) → broker returns message batch
  3. Process messages
  4. ACK(last_offset) → broker commits offset
  5. If disconnect before ACK, next FETCH starts from last committed offset → redelivery
"""

import asyncio
import logging
from typing import Awaitable, Callable, Optional

from deb.protocol.frame import Cmd, Frame

logger = logging.getLogger("deb.client.consumer")


class Consumer:
    """Async consumer client with at-least-once delivery."""

    def __init__(
        self,
        broker_host: str = "127.0.0.1",
        broker_port: int = 9090,
        group: str = "default",
        fetch_max_bytes: int = 65536,
        poll_interval: float = 0.5,
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.group = group
        self.fetch_max_bytes = fetch_max_bytes
        self.poll_interval = poll_interval
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._offset: int = 0
        self._running = False

    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self.broker_host, self.broker_port
        )
        logger.info("Consumer connected to %s:%d", self.broker_host, self.broker_port)

    async def close(self):
        self._running = False
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except OSError:
                pass
        logger.info("Consumer disconnected")

    async def subscribe(self, topic: str) -> int:
        """Subscribe to a topic. Returns the starting offset."""
        if not self._writer:
            raise ConnectionError("Not connected")

        frame = Frame(Cmd.SUBSCRIBE, {"topic": topic, "group": self.group})
        self._writer.write(frame.encode())
        await self._writer.drain()

        resp = await Frame.read_from(self._reader)
        if resp.cmd == Cmd.OFFSET_REPLY:
            self._offset = resp.payload["offset"]
            logger.info("Subscribed to %s (group=%s, offset=%d)", topic, self.group, self._offset)
            return self._offset
        elif resp.cmd == Cmd.ERROR:
            raise RuntimeError(f"Broker error: {resp.payload}")
        else:
            raise RuntimeError(f"Unexpected response: {resp.cmd.name}")

    async def fetch(self, topic: str) -> list[dict]:
        """Fetch a batch of messages from current offset."""
        if not self._writer:
            raise ConnectionError("Not connected")

        frame = Frame(Cmd.FETCH, {
            "topic": topic,
            "group": self.group,
            "offset": self._offset,
            "max_bytes": self.fetch_max_bytes,
        })
        self._writer.write(frame.encode())
        await self._writer.drain()

        resp = await Frame.read_from(self._reader)
        if resp.cmd == Cmd.MESSAGE_BATCH:
            return resp.payload.get("messages", [])
        elif resp.cmd == Cmd.ERROR:
            raise RuntimeError(f"Broker error: {resp.payload}")
        else:
            raise RuntimeError(f"Unexpected response: {resp.cmd.name}")

    async def commit(self, topic: str, offset: int):
        """Acknowledge processed offset (at-least-once commit)."""
        if not self._writer:
            raise ConnectionError("Not connected")

        frame = Frame(Cmd.ACK, {
            "topic": topic,
            "group": self.group,
            "offset": offset,
        })
        self._writer.write(frame.encode())
        await self._writer.drain()

        resp = await Frame.read_from(self._reader)
        if resp.cmd == Cmd.ACK:
            self._offset = offset + 1  # advance past committed
            logger.debug("Committed offset %d for %s/%s", offset, topic, self.group)
        elif resp.cmd == Cmd.ERROR:
            logger.error("Commit failed: %s", resp.payload)

    async def consume(self, topic: str, handler: Callable[[dict], Awaitable]):
        """Continuous consume loop: fetch → handle → commit."""
        self._running = True
        while self._running:
            try:
                messages = await self.fetch(topic)
                if not messages:
                    await asyncio.sleep(self.poll_interval)
                    continue
                for msg in messages:
                    await handler(msg)
                # Commit last message offset
                last_offset = messages[-1]["offset"]
                await self.commit(topic, last_offset)
            except (ConnectionError, OSError) as e:
                logger.warning("Connection lost: %s — reconnecting in 2s", e)
                await asyncio.sleep(2.0)
                try:
                    await self.connect()
                    await self.subscribe(topic)
                except (ConnectionError, OSError):
                    continue
            except asyncio.CancelledError:
                break

    async def stop_consuming(self):
        self._running = False

