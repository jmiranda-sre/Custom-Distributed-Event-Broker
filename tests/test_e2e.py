"""End-to-end tests for the Custom Distributed Event Broker."""

import asyncio
import os
import shutil
import struct
import tempfile
import time
import unittest

from deb.log.commit_log import CommitLog, Segment, RECORD_HEADER_FMT, RECORD_HEADER_SIZE
from deb.broker.offset_manager import OffsetManager
from deb.protocol.frame import Cmd, Frame, HEADER_FMT, HEADER_SIZE
from deb.broker.server import BrokerServer
from deb.client.producer import Producer
from deb.client.consumer import Consumer


class TestFrame(unittest.TestCase):
    """Test binary wire protocol frame encode/decode."""

    def test_encode_decode_roundtrip(self):
        frame = Frame(Cmd.PRODUCE, {"topic": "test", "key": "k1", "value": "hello"})
        data = frame.encode()
        # Header: 4B payload_len + 1B cmd
        payload_len, cmd = struct.unpack(HEADER_FMT, data[:HEADER_SIZE])
        self.assertEqual(cmd, Cmd.PRODUCE)
        self.assertEqual(payload_len, len(data) - HEADER_SIZE)

    async def _read_frame(self, data: bytes) -> Frame:
        """Helper: read frame from bytes via asyncio stream."""
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        return await Frame.read_from(reader)

    def test_roundtrip_produce(self):
        async def _test():
            frame = Frame(Cmd.PRODUCE, {"topic": "t", "value": "v"})
            data = frame.encode()
            result = await self._read_frame(data)
            self.assertEqual(result.cmd, Cmd.PRODUCE)
            self.assertEqual(result.payload["topic"], "t")
            self.assertEqual(result.payload["value"], "v")

        asyncio.run(_test())

    def test_roundtrip_heartbeat(self):
        async def _test():
            frame = Frame(Cmd.HEARTBEAT, {"ts": 1234.5})
            data = frame.encode()
            result = await self._read_frame(data)
            self.assertEqual(result.cmd, Cmd.HEARTBEAT)
            self.assertAlmostEqual(result.payload["ts"], 1234.5)

        asyncio.run(_test())


class TestSegment(unittest.TestCase):
    """Test single log segment read/write."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_append_and_read(self):
        from pathlib import Path
        seg = Segment(Path(self.tmpdir), 0, max_bytes=1024 * 1024)
        entry = seg.append(b"key1", b"value1")
        self.assertEqual(entry.offset, 0)
        self.assertEqual(entry.key, b"key1")
        self.assertEqual(entry.value, b"value1")

        read_back = seg.read(0)
        self.assertIsNotNone(read_back)
        self.assertEqual(read_back.value, b"value1")
        self.assertEqual(read_back.key, b"key1")
        seg.close()

    def test_read_range(self):
        from pathlib import Path
        seg = Segment(Path(self.tmpdir), 0, max_bytes=1024 * 1024)
        for i in range(10):
            seg.append(None, f"msg-{i}".encode())
        entries = seg.read_range(3, max_bytes=65536)
        self.assertEqual(len(entries), 7)
        self.assertEqual(entries[0].offset, 3)
        self.assertEqual(entries[0].value, b"msg-3")
        seg.close()

    def test_segment_full_roll(self):
        from pathlib import Path
        seg = Segment(Path(self.tmpdir), 0, max_bytes=100)  # small segment
        seg.append(None, b"short")
        self.assertFalse(seg.is_full)
        # Write enough to fill
        for i in range(20):
            seg.append(None, b"x" * 50)
        self.assertTrue(seg.is_full)
        seg.close()

    def test_recover_from_disk(self):
        from pathlib import Path
        seg = Segment(Path(self.tmpdir), 0, max_bytes=1024 * 1024)
        seg.append(b"k", b"v1")
        seg.append(None, b"v2")
        seg.close()

        # Reopen
        seg2 = Segment(Path(self.tmpdir), 0, max_bytes=1024 * 1024)
        e0 = seg2.read(0)
        e1 = seg2.read(1)
        self.assertIsNotNone(e0)
        self.assertEqual(e0.value, b"v1")
        self.assertEqual(e0.key, b"k")
        self.assertIsNotNone(e1)
        self.assertEqual(e1.value, b"v2")
        seg2.close()


class TestCommitLog(unittest.TestCase):
    """Test multi-segment commit log."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_append_and_read(self):
        log = CommitLog(self.tmpdir, "test-topic", max_segment_bytes=1024 * 1024)
        e0 = log.append(b"key0", b"val0")
        e1 = log.append(None, b"val1")
        self.assertEqual(e0.offset, 0)
        self.assertEqual(e1.offset, 1)

        r0 = log.read(0)
        self.assertEqual(r0.value, b"val0")
        r1 = log.read(1)
        self.assertEqual(r1.value, b"val1")
        log.close()

    def test_segment_rolling(self):
        log = CommitLog(self.tmpdir, "roll-topic", max_segment_bytes=200)
        for i in range(20):
            log.append(None, f"message-number-{i:03d}".encode())
        # Should have multiple segments
        self.assertGreater(len(log._segments), 1)

        # Read across segment boundary
        entries = log.read_range(0, max_bytes=100000)
        self.assertEqual(len(entries), 20)
        log.close()

    def test_persistence_across_restart(self):
        log = CommitLog(self.tmpdir, "persist-topic", max_segment_bytes=1024 * 1024)
        for i in range(5):
            log.append(None, f"persistent-{i}".encode())
        log.close()

        # Reopen
        log2 = CommitLog(self.tmpdir, "persist-topic", max_segment_bytes=1024 * 1024)
        self.assertEqual(log2.next_offset, 5)
        entries = log2.read_range(0, max_bytes=100000)
        self.assertEqual(len(entries), 5)
        self.assertEqual(entries[3].value, b"persistent-3")
        log2.close()


class TestOffsetManager(unittest.TestCase):
    """Test consumer offset tracking."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_commit_and_get(self):
        mgr = OffsetManager(self.tmpdir)
        self.assertEqual(mgr.get_next_offset("t1", "g1"), 0)
        mgr.commit("t1", "g1", 5)
        self.assertEqual(mgr.get_last_committed("t1", "g1"), 5)
        self.assertEqual(mgr.get_next_offset("t1", "g1"), 6)
        mgr.commit("t1", "g1", 3)  # stale commit — should be ignored
        self.assertEqual(mgr.get_last_committed("t1", "g1"), 5)
        mgr.close()

    def test_persistence(self):
        mgr = OffsetManager(self.tmpdir)
        mgr.commit("t1", "g1", 10)
        mgr.commit("t2", "g1", 20)
        mgr.close()

        mgr2 = OffsetManager(self.tmpdir)
        self.assertEqual(mgr2.get_last_committed("t1", "g1"), 10)
        self.assertEqual(mgr2.get_last_committed("t2", "g1"), 20)
        mgr2.close()


class TestBrokerE2E(unittest.TestCase):
    """End-to-end test: broker + producer + consumer over TCP."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.broker = BrokerServer(
            host="127.0.0.1",
            port=0,  # OS-assigned port
            data_dir=self.tmpdir,
        )

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    async def _start_broker(self):
        await self.broker.start()
        # Get assigned port
        addr = self.broker._server.sockets[0].getsockname()
        return addr[0], addr[1]

    def test_produce_and_consume(self):
        async def _test():
            host, port = await self._start_broker()

            # Produce messages
            producer = Producer(broker_host=host, broker_port=port)
            await producer.connect()
            offsets = []
            for i in range(5):
                off = await producer.send("test-topic", f"hello-{i}", key=f"k{i}")
                offsets.append(off)
            await producer.close()

            self.assertEqual(offsets, [0, 1, 2, 3, 4])

            # Consume messages
            consumer = Consumer(broker_host=host, broker_port=port, group="test-group")
            await consumer.connect()
            start_offset = await consumer.subscribe("test-topic")
            self.assertEqual(start_offset, 0)

            messages = await consumer.fetch("test-topic")
            self.assertEqual(len(messages), 5)
            self.assertEqual(messages[0]["value"], "hello-0")
            self.assertEqual(messages[4]["value"], "hello-4")

            # Commit offset 4 (last message)
            await consumer.commit("test-topic", 4)

            # Fetch again — should get nothing new
            messages2 = await consumer.fetch("test-topic")
            self.assertEqual(len(messages2), 0)

            await consumer.close()
            await self.broker.stop()

        asyncio.run(_test())

    def test_at_least_once_redelivery(self):
        """Verify that uncommitted messages are redelivered on reconnect."""
        async def _test():
            host, port = await self._start_broker()

            producer = Producer(broker_host=host, broker_port=port)
            await producer.connect()
            await producer.send("redeliver-topic", "msg-1")
            await producer.send("redeliver-topic", "msg-2")
            await producer.close()

            # Consume but DON'T commit
            consumer1 = Consumer(broker_host=host, broker_port=port, group="redeliver-group")
            await consumer1.connect()
            await consumer1.subscribe("redeliver-topic")
            msgs = await consumer1.fetch("redeliver-topic")
            self.assertEqual(len(msgs), 2)
            # Disconnect without committing
            await consumer1.close()

            # Reconnect as same consumer group — should get same messages
            consumer2 = Consumer(broker_host=host, broker_port=port, group="redeliver-group")
            await consumer2.connect()
            start = await consumer2.subscribe("redeliver-topic")
            self.assertEqual(start, 0)  # offset unchanged since no commit
            msgs2 = await consumer2.fetch("redeliver-topic")
            self.assertEqual(len(msgs2), 2)
            self.assertEqual(msgs2[0]["value"], "msg-1")

            # Now commit and verify no redelivery
            await consumer2.commit("redeliver-topic", 1)
            await consumer2.close()

            consumer3 = Consumer(broker_host=host, broker_port=port, group="redeliver-group")
            await consumer3.connect()
            start3 = await consumer3.subscribe("redeliver-topic")
            self.assertEqual(start3, 2)  # advanced past committed
            await consumer3.close()

            await self.broker.stop()

        asyncio.run(_test())

    def test_broker_restart_preserves_data(self):
        """Verify data survives broker restart."""
        async def _test():
            host, port = await self._start_broker()

            producer = Producer(broker_host=host, broker_port=port)
            await producer.connect()
            await producer.send("restart-topic", "before-restart")
            await producer.close()

            await self.broker.stop()

            # Restart broker on a new port (avoid TIME_WAIT reuse issues) with same data dir
            self.broker = BrokerServer(
                host="127.0.0.1", port=0, data_dir=self.tmpdir
            )
            await self.broker.start()
            addr2 = self.broker._server.sockets[0].getsockname()
            host2, port2 = addr2[0], addr2[1]

            # Produce more
            producer2 = Producer(broker_host=host2, broker_port=port2)
            await producer2.connect()
            off = await producer2.send("restart-topic", "after-restart")
            self.assertEqual(off, 1)  # continues from offset 1
            await producer2.close()

            # Read both
            consumer = Consumer(broker_host=host2, broker_port=port2, group="grp")
            await consumer.connect()
            await consumer.subscribe("restart-topic")
            msgs = await consumer.fetch("restart-topic")
            self.assertEqual(len(msgs), 2)
            self.assertEqual(msgs[0]["value"], "before-restart")
            self.assertEqual(msgs[1]["value"], "after-restart")
            await consumer.close()

            await self.broker.stop()

        asyncio.run(_test())

    def test_multiple_consumer_groups(self):
        """Different groups have independent offsets."""
        async def _test():
            host, port = await self._start_broker()

            producer = Producer(broker_host=host, broker_port=port)
            await producer.connect()
            await producer.send("multi-topic", "msg-A")
            await producer.close()

            # Group 1: consume and commit
            c1 = Consumer(broker_host=host, broker_port=port, group="group-1")
            await c1.connect()
            await c1.subscribe("multi-topic")
            msgs1 = await c1.fetch("multi-topic")
            await c1.commit("multi-topic", msgs1[-1]["offset"])
            await c1.close()

            # Group 2: never consumed — offset should be 0
            c2 = Consumer(broker_host=host, broker_port=port, group="group-2")
            await c2.connect()
            start2 = await c2.subscribe("multi-topic")
            self.assertEqual(start2, 0)
            msgs2 = await c2.fetch("multi-topic")
            self.assertEqual(len(msgs2), 1)
            await c2.close()

            await self.broker.stop()

        asyncio.run(_test())


class TestFailover(unittest.TestCase):
    """Test leader/follower failover between two broker nodes."""

    def setUp(self):
        self.tmpdir_leader = tempfile.mkdtemp()
        self.tmpdir_follower = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir_leader, ignore_errors=True)
        shutil.rmtree(self.tmpdir_follower, ignore_errors=True)

    def test_produce_on_leader_rejected_on_follower(self):
        """Writes to follower should be rejected; writes to leader succeed."""
        async def _test():
            from deb.failover.failover import FailoverModule

            leader_failover = FailoverModule(
                node_id="leader",
                role="leader",
                peer_host="127.0.0.1",
                peer_port=9092,
                auto_failover=False,
                heartbeat_interval=1.0,
            )
            follower_failover = FailoverModule(
                node_id="follower",
                role="follower",
                peer_host="127.0.0.1",
                peer_port=9091,
                auto_failover=False,
                heartbeat_interval=1.0,
            )

            leader = BrokerServer(
                host="127.0.0.1", port=0, data_dir=self.tmpdir_leader,
                failover=leader_failover,
            )
            follower = BrokerServer(
                host="127.0.0.1", port=0, data_dir=self.tmpdir_follower,
                failover=follower_failover,
            )
            await leader.start()
            await follower.start()

            l_addr = leader._server.sockets[0].getsockname()
            f_addr = follower._server.sockets[0].getsockname()
            l_host, l_port = l_addr[0], l_addr[1]
            f_host, f_port = f_addr[0], f_addr[1]

            # Produce on leader — should succeed
            p = Producer(broker_host=l_host, broker_port=l_port)
            await p.connect()
            off = await p.send("failover-topic", "on-leader")
            self.assertEqual(off, 0)
            await p.close()

            # Produce on follower — should fail
            p2 = Producer(broker_host=f_host, broker_port=f_port)
            await p2.connect()
            with self.assertRaises(RuntimeError):
                await p2.send("failover-topic", "on-follower")
            await p2.close()

            await leader.stop()
            await follower.stop()

        asyncio.run(_test())

    def test_follower_promotion_on_timeout(self):
        """Follower promotes to leader after heartbeat timeout."""
        async def _test():
            from deb.failover.failover import FailoverModule, NodeRole

            leader_failover = FailoverModule(
                node_id="leader",
                role="leader",
                peer_host="127.0.0.1",
                peer_port=0,  # unreachable
                auto_failover=True,
                heartbeat_interval=0.5,
                election_timeout=2.0,
            )
            follower_failover = FailoverModule(
                node_id="follower",
                role="follower",
                peer_host="127.0.0.1",
                peer_port=0,  # unreachable
                auto_failover=True,
                heartbeat_interval=0.5,
                election_timeout=2.0,
            )

            follower = BrokerServer(
                host="127.0.0.1", port=0, data_dir=self.tmpdir_follower,
                failover=follower_failover,
            )
            await follower.start()

            # Follower should be FOLLOWER initially
            self.assertEqual(follower_failover.role, NodeRole.FOLLOWER)

            # Wait for election timeout
            await asyncio.sleep(3.0)

            # Follower should have promoted itself
            self.assertEqual(follower_failover.role, NodeRole.LEADER)
            self.assertGreater(follower_failover.term, 0)

            # Should now accept writes as leader
            f_addr = follower._server.sockets[0].getsockname()
            f_host, f_port = f_addr[0], f_addr[1]
            p = Producer(broker_host=f_host, broker_port=f_port)
            await p.connect()
            off = await p.send("promoted-topic", "after-promotion")
            self.assertEqual(off, 0)
            await p.close()

            await follower.stop()

        asyncio.run(_test())


class TestRetention(unittest.TestCase):
    """Test log retention / segment cleanup."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_cleanup_removes_old_segments(self):
        """Segments older than retention_ms are deleted; active segment kept."""
        # Use very small segments to force rolling
        log = CommitLog(self.tmpdir, "retention-topic", max_segment_bytes=200,
                        retention_ms=500)
        # Write enough to create 3+ segments
        for i in range(30):
            log.append(None, f"msg-{i:04d}".encode())
        num_segs_before = len(log._segments)
        self.assertGreaterEqual(num_segs_before, 3)

        # Wait for segments to age out
        time.sleep(0.6)

        removed = log.cleanup()
        self.assertGreater(removed, 0)
        # Active segment must survive
        self.assertGreaterEqual(len(log._segments), 1)
        # The active segment's base_offset should match the last segment
        self.assertEqual(log._segments[-1], log._active_segment)

        log.close()

    def test_cleanup_noop_when_disabled(self):
        """With retention_ms=None, cleanup does nothing."""
        log = CommitLog(self.tmpdir, "no-retention", max_segment_bytes=200)
        for i in range(30):
            log.append(None, f"msg-{i:04d}".encode())
        num_segs = len(log._segments)
        self.assertGreaterEqual(num_segs, 3)

        removed = log.cleanup()
        self.assertEqual(removed, 0)
        self.assertEqual(len(log._segments), num_segs)
        log.close()


if __name__ == "__main__":
    unittest.main()
