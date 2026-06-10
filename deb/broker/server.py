"""Broker server — asyncio TCP server handling all client connections.

Responsibilities:
  - Accept producer/consumer/follower TCP connections
  - Route PRODUCE → CommitLog.append()
  - Route FETCH → CommitLog.read_range() + at-least-once delivery
  - Route ACK → OffsetManager.commit()
  - Route SUBSCRIBE → register consumer group
  - Heartbeat loop for connection keep-alive
  - Integration with FailoverModule for leader/follower behavior
"""

import asyncio
import logging
import time
from typing import Optional

from deb.protocol.frame import Cmd, Frame
from deb.log.commit_log import CommitLog
from deb.broker.offset_manager import OffsetManager
from deb.failover.failover import FailoverModule

logger = logging.getLogger("deb.broker")


class BrokerServer:
    """Async TCP broker server."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 9090,
        data_dir: str = "./data",
        max_segment_bytes: int = 1024 * 1024,
        fetch_max_bytes: int = 65536,
        heartbeat_interval: float = 5.0,
        failover: Optional[FailoverModule] = None,
        retention_ms: Optional[int] = None,
        cleanup_interval: float = 60.0,
    ):
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.max_segment_bytes = max_segment_bytes
        self.fetch_max_bytes = fetch_max_bytes
        self.heartbeat_interval = heartbeat_interval
        self.failover = failover
        self.retention_ms = retention_ms
        self.cleanup_interval = cleanup_interval

        self._logs: dict[str, CommitLog] = {}
        self._offset_mgr = OffsetManager(data_dir)
        self._server: Optional[asyncio.Server] = None
        self._connections: set[asyncio.Task] = set()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False

    def _get_log(self, topic: str) -> CommitLog:
        if topic not in self._logs:
            self._logs[topic] = CommitLog(
                self.data_dir, topic, self.max_segment_bytes, self.retention_ms
            )
        return self._logs[topic]

    async def start(self):
        self._running = True
        self._server = await asyncio.start_server(
            self._handle_connection, self.host, self.port
        )
        if self.failover:
            await self.failover.start(self)
        if self.retention_ms is not None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Broker listening on %s:%d", self.host, self.port)

    async def serve(self):
        """Start and serve forever."""
        await self.start()
        async with self._server:
            await self._server.serve_forever()

    async def stop(self):
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        if self.failover:
            await self.failover.stop()
        for task in self._connections:
            task.cancel()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for log in self._logs.values():
            log.close()
        self._offset_mgr.close()
        logger.info("Broker stopped")

    async def _cleanup_loop(self):
        """Periodically run retention cleanup on all topic logs."""
        while self._running:
            await asyncio.sleep(self.cleanup_interval)
            for topic, log in list(self._logs.items()):
                try:
                    removed = log.cleanup()
                    if removed:
                        logger.info("Retention: removed %d old segment(s) from '%s'", removed, topic)
                except Exception:
                    logger.exception("Retention cleanup failed for '%s'", topic)

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        logger.info("Connection from %s", addr)
        task = asyncio.current_task()
        self._connections.add(task)

        try:
            while self._running:
                try:
                    frame = await asyncio.wait_for(
                        Frame.read_from(reader), timeout=self.heartbeat_interval * 3
                    )
                except asyncio.TimeoutError:
                    # Send heartbeat probe
                    try:
                        hb = Frame(Cmd.HEARTBEAT, {"ts": time.time()})
                        writer.write(hb.encode())
                        await writer.drain()
                        continue
                    except (ConnectionError, OSError):
                        break
                except ConnectionError:
                    break

                resp = await self._dispatch(frame, writer)
                if resp:
                    writer.write(resp.encode())
                    await writer.drain()

        except (asyncio.CancelledError, ConnectionError, OSError):
            pass
        finally:
            self._connections.discard(task)
            try:
                writer.close()
                await writer.wait_closed()
            except OSError:
                pass
            logger.info("Disconnected %s", addr)

    async def _dispatch(self, frame: Frame, writer: asyncio.StreamWriter) -> Optional[Frame]:
        cmd = frame.cmd
        p = frame.payload

        if cmd == Cmd.PRODUCE:
            return await self._handle_produce(p)
        elif cmd == Cmd.SUBSCRIBE:
            return await self._handle_subscribe(p)
        elif cmd == Cmd.FETCH:
            return await self._handle_fetch(p)
        elif cmd == Cmd.ACK:
            return await self._handle_ack(p)
        elif cmd == Cmd.HEARTBEAT:
            return Frame(Cmd.HEARTBEAT, {"ts": time.time()})
        elif cmd == Cmd.REPLICATE:
            return await self._handle_replicate(p)
        elif cmd == Cmd.LEADER_ANNOUNCE:
            if self.failover:
                await self.failover.on_leader_announce(p)
            return None
        else:
            return Frame(Cmd.ERROR, {"code": 400, "message": f"Unknown command {cmd}"})

    async def _handle_produce(self, p: dict) -> Frame:
        topic = p.get("topic")
        if not topic:
            return Frame(Cmd.ERROR, {"code": 400, "message": "Missing topic"})

        # Reject writes if follower
        if self.failover and not self.failover.is_leader:
            return Frame(Cmd.ERROR, {"code": 503, "message": "Not leader"})

        key = p.get("key")
        value = p.get("value", "")
        log = self._get_log(topic)
        entry = log.append(key.encode() if key else None, value.encode())

        # Replicate to follower
        if self.failover and self.failover.is_leader:
            await self.failover.replicate_entry(topic, entry)

        return Frame(Cmd.PRODUCE_ACK, {"topic": topic, "offset": entry.offset})

    async def _handle_subscribe(self, p: dict) -> Frame:
        topic = p.get("topic", "")
        group = p.get("group", "")
        if not topic or not group:
            return Frame(Cmd.ERROR, {"code": 400, "message": "Missing topic or group"})
        # Ensure topic log exists
        self._get_log(topic)
        next_offset = self._offset_mgr.get_next_offset(topic, group)
        return Frame(Cmd.OFFSET_REPLY, {"topic": topic, "group": group, "offset": next_offset})

    async def _handle_fetch(self, p: dict) -> Frame:
        topic = p.get("topic", "")
        group = p.get("group", "")
        offset = p.get("offset", 0)
        max_bytes = p.get("max_bytes", self.fetch_max_bytes)

        if not topic:
            return Frame(Cmd.ERROR, {"code": 400, "message": "Missing topic"})

        log = self._get_log(topic)
        entries = log.read_range(offset, max_bytes)
        messages = [e.to_dict() for e in entries]
        return Frame(Cmd.MESSAGE_BATCH, {"topic": topic, "messages": messages})

    async def _handle_ack(self, p: dict) -> Frame:
        topic = p.get("topic", "")
        group = p.get("group", "")
        offset = p.get("offset", 0)
        if not topic or not group:
            return Frame(Cmd.ERROR, {"code": 400, "message": "Missing topic or group"})
        self._offset_mgr.commit(topic, group, offset)
        return Frame(Cmd.ACK, {"topic": topic, "group": group, "offset": offset})

    async def _handle_replicate(self, p: dict) -> Frame:
        """Handle incoming replication from leader (follower only)."""
        topic = p.get("topic", "")
        entry = p.get("entry", {})
        if not topic or not entry:
            return Frame(Cmd.ERROR, {"code": 400, "message": "Invalid replicate payload"})

        log = self._get_log(topic)
        key = entry.get("key")
        value = entry.get("value", "")
        rec = log.append(key.encode() if key else None, value.encode())
        return Frame(Cmd.REPLICATE_ACK, {"topic": topic, "offset": rec.offset})

    # --- Failover integration ---

    def get_log_for_replication(self, topic: str, offset: int):
        """Read entries for replication (used by FailoverModule)."""
        log = self._get_log(topic)
        return log.read_range(offset, self.fetch_max_bytes)

    def replicate_entry_sync(self, topic: str, key: Optional[bytes], value: bytes):
        """Called by FailoverModule to write a replicated entry to local log."""
        log = self._get_log(topic)
        return log.append(key, value)
