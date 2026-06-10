"""Failover module — leader/follower with heartbeat-based detection.

Architecture:
  - Two-node cluster: leader + follower
  - Leader sends HEARTBEAT (with LEADER_ANNOUNCE) to follower at regular intervals
  - Follower monitors leader heartbeats; if missing for > election_timeout, promotes itself
  - Leader also replicates all PRODUCE entries to follower via REPLICATE frames
  - Promotion can be semi-automatic (configurable) or require --force flag

State machine:
  LEADER ──(lose quorum)──→ CANDIDATE ──(win election)──→ LEADER
  FOLLOWER ──(timeout)──→ CANDIDATE ──(become leader)──→ LEADER
  For simplicity with 2 nodes: FOLLOWER → LEADER on timeout.
"""

import asyncio
import logging
import time
from typing import Optional
from enum import Enum

logger = logging.getLogger("deb.failover")


class NodeRole(Enum):
    LEADER = "leader"
    FOLLOWER = "follower"
    CANDIDATE = "candidate"


class FailoverModule:
    """Manages leader/follower failover for a two-node cluster."""

    def __init__(
        self,
        node_id: str,
        role: str = "follower",
        peer_host: str = "127.0.0.1",
        peer_port: int = 9091,
        heartbeat_interval: float = 2.0,
        election_timeout: float = 6.0,
        auto_failover: bool = True,
    ):
        self.node_id = node_id
        self.role = NodeRole(role)
        self.peer_host = peer_host
        self.peer_port = peer_port
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout = election_timeout
        self.auto_failover = auto_failover

        self.term = 0
        self._last_leader_heartbeat = 0.0
        self._broker = None  # set by start()
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._peer_writer: Optional[asyncio.StreamWriter] = None
        self._peer_reader: Optional[asyncio.StreamReader] = None
        self._replication_queue: asyncio.Queue = asyncio.Queue()

    @property
    def is_leader(self) -> bool:
        return self.role == NodeRole.LEADER

    async def start(self, broker):
        """Start failover module, attached to a BrokerServer."""
        self._broker = broker
        self._running = True
        self._last_leader_heartbeat = time.time()

        if self.role == NodeRole.LEADER:
            self._tasks.append(asyncio.create_task(self._leader_heartbeat_loop()))
            self._tasks.append(asyncio.create_task(self._replication_loop()))
        else:
            self._tasks.append(asyncio.create_task(self._follower_monitor_loop()))
            self._tasks.append(asyncio.create_task(self._replication_loop()))

        logger.info("Failover started as %s (node=%s)", self.role.value, self.node_id)

    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._close_peer()
        logger.info("Failover stopped (node=%s)", self.node_id)

    async def on_leader_announce(self, payload: dict):
        """Called when a LEADER_ANNOUNCE frame is received."""
        leader_id = payload.get("leader_id", "")
        term = payload.get("term", 0)
        if term >= self.term and leader_id != self.node_id:
            self.term = term
            self._last_leader_heartbeat = time.time()
            if self.role != NodeRole.FOLLOWER:
                self.role = NodeRole.FOLLOWER
                logger.info("Stepping down to FOLLOWER (leader=%s term=%d)", leader_id, term)

    async def replicate_entry(self, topic: str, entry):
        """Queue a log entry for replication to follower (called by leader broker)."""
        await self._replication_queue.put((topic, entry))

    # --- Internal loops ---

    async def _leader_heartbeat_loop(self):
        """Leader periodically sends LEADER_ANNOUNCE to follower."""
        while self._running:
            try:
                await self._ensure_peer_connection()
                if self._peer_writer and not self._peer_writer.is_closing():
                    from deb.protocol.frame import Cmd, Frame
                    announce = Frame(Cmd.LEADER_ANNOUNCE, {
                        "leader_id": self.node_id,
                        "term": self.term,
                    })
                    self._peer_writer.write(announce.encode())
                    await self._peer_writer.drain()
                else:
                    logger.warning("No peer connection for heartbeat")
            except (ConnectionError, OSError, asyncio.CancelledError):
                logger.warning("Failed to send heartbeat to peer")
                self._close_peer()
            await asyncio.sleep(self.heartbeat_interval)

    async def _follower_monitor_loop(self):
        """Follower monitors leader heartbeats; promotes on timeout."""
        while self._running:
            elapsed = time.time() - self._last_leader_heartbeat
            if elapsed > self.election_timeout:
                if self.auto_failover:
                    logger.warning(
                        "Leader heartbeat timeout (%.1fs > %.1fs) — promoting to LEADER",
                        elapsed, self.election_timeout,
                    )
                    await self._promote_to_leader()
                else:
                    logger.warning(
                        "Leader heartbeat timeout (%.1fs) — auto_failover=False, waiting",
                        elapsed,
                    )
                    # Reset timer to avoid log spam
                    self._last_leader_heartbeat = time.time()
            await asyncio.sleep(1.0)

    async def _promote_to_leader(self):
        """Promote this node from follower to leader."""
        self.term += 1
        self.role = NodeRole.LEADER
        self._close_peer()
        # Cancel follower monitor, start leader loops
        for t in list(self._tasks):
            t.cancel()
        self._tasks.clear()
        await asyncio.sleep(0.1)  # let cancellations settle
        self._tasks.append(asyncio.create_task(self._leader_heartbeat_loop()))
        self._tasks.append(asyncio.create_task(self._replication_loop()))
        logger.info("Promoted to LEADER (term=%d, node=%s)", self.term, self.node_id)

    async def _replication_loop(self):
        """Send queued replication entries to follower (leader) or process them (follower)."""
        while self._running:
            try:
                topic, entry = await asyncio.wait_for(
                    self._replication_queue.get(), timeout=2.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            if not self.is_leader:
                continue

            try:
                await self._ensure_peer_connection()
                if self._peer_writer and not self._peer_writer.is_closing():
                    from deb.protocol.frame import Cmd, Frame
                    rep = Frame(Cmd.REPLICATE, {
                        "topic": topic,
                        "entry": entry.to_dict(),
                    })
                    self._peer_writer.write(rep.encode())
                    await self._peer_writer.drain()
            except (ConnectionError, OSError):
                logger.warning("Failed to replicate to peer — entry queued")
                self._close_peer()
                # Re-queue (at-least-once: we keep trying)
                await self._replication_queue.put((topic, entry))
                await asyncio.sleep(1.0)

    # --- Peer connection management ---

    async def _ensure_peer_connection(self):
        """Ensure TCP connection to peer node."""
        if self._peer_writer and not self._peer_writer.is_closing():
            return
        try:
            self._peer_reader, self._peer_writer = await asyncio.open_connection(
                self.peer_host, self.peer_port, limit=1024 * 1024
            )
            logger.info("Connected to peer %s:%d", self.peer_host, self.peer_port)
            # Start reading peer responses in background
            asyncio.create_task(self._read_peer_loop())
        except (ConnectionError, OSError):
            self._peer_writer = None
            self._peer_reader = None

    async def _read_peer_loop(self):
        """Read frames from peer connection."""
        from deb.protocol.frame import Frame, Cmd
        try:
            while self._running and self._peer_reader:
                frame = await Frame.read_from(self._peer_reader)
                if frame.cmd == Cmd.LEADER_ANNOUNCE:
                    await self.on_leader_announce(frame.payload)
                elif frame.cmd == Cmd.HEARTBEAT:
                    self._last_leader_heartbeat = time.time()
                elif frame.cmd == Cmd.REPLICATE_ACK:
                    logger.debug("Replication confirmed: %s", frame.payload)
        except (ConnectionError, asyncio.CancelledError, OSError):
            pass
        finally:
            self._close_peer()

    def _close_peer(self):
        if self._peer_writer:
            try:
                self._peer_writer.close()
            except OSError:
                pass
            self._peer_writer = None
            self._peer_reader = None
