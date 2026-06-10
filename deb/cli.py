"""CLI entry points for the Custom Distributed Event Broker."""

import argparse
import asyncio
import logging
import sys

from deb.broker.server import BrokerServer
from deb.failover.failover import FailoverModule


def configure_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_broker(args):
    """Start the broker server."""
    configure_logging(args.log_level)

    failover = None
    if args.peer_host:
        failover = FailoverModule(
            node_id=args.node_id,
            role=args.role,
            peer_host=args.peer_host,
            peer_port=args.peer_port,
            heartbeat_interval=args.heartbeat_interval,
            election_timeout=args.election_timeout,
            auto_failover=args.auto_failover,
        )

    broker = BrokerServer(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        max_segment_bytes=args.max_segment_bytes,
        failover=failover,
        retention_ms=args.retention_ms,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(broker.serve())
    except KeyboardInterrupt:
        loop.run_until_complete(broker.stop())
    finally:
        loop.close()


def run_producer(args):
    """Run a producer that sends messages from stdin or arguments."""
    configure_logging(args.log_level)

    async def _run():
        from deb.client.producer import Producer
        producer = Producer(broker_host=args.host, broker_port=args.port)
        await producer.connect()
        try:
            if args.message:
                offset = await producer.send(args.topic, args.message, key=args.key)
                print(f"→ sent to '{args.topic}' at offset {offset}")
            elif args.file:
                with open(args.file, "r") as f:
                    for line in f:
                        line = line.rstrip("\n")
                        if line:
                            offset = await producer.send(args.topic, line, key=args.key)
                            print(f"→ offset {offset}: {line[:60]}")
            else:
                # Interactive mode — read from stdin
                print(f"Producer for topic '{args.topic}' — type messages (Ctrl+D to exit):")
                loop = asyncio.get_event_loop()
                while True:
                    try:
                        line = await loop.run_in_executor(None, input, "> ")
                        if not line:
                            continue
                        offset = await producer.send(args.topic, line, key=args.key)
                        print(f"  ✓ offset {offset}")
                    except EOFError:
                        break
        finally:
            await producer.close()

    asyncio.run(_run())


def run_consumer(args):
    """Run a consumer that processes messages from a topic."""
    configure_logging(args.log_level)

    async def _run():
        from deb.client.consumer import Consumer
        consumer = Consumer(
            broker_host=args.host,
            broker_port=args.port,
            group=args.group,
            fetch_max_bytes=args.max_bytes,
            poll_interval=args.poll_interval,
        )
        await consumer.connect()
        offset = await consumer.subscribe(args.topic)
        print(f"Consumer for '{args.topic}' (group={args.group}) starting at offset {offset}")

        async def handler(msg):
            key = msg.get("key") or ""
            val = msg.get("value", "")
            print(f"[{msg['offset']}] {key}: {val}" if key else f"[{msg['offset']}] {val}")

        try:
            await consumer.consume(args.topic, handler)
        except KeyboardInterrupt:
            pass
        finally:
            await consumer.close()

    asyncio.run(_run())


def main():
    parser = argparse.ArgumentParser(
        prog="deb",
        description="Custom Distributed Event Broker",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    sub = parser.add_subparsers(dest="command", required=True)

    # --- broker ---
    b = sub.add_parser("broker", help="Start the broker server")
    b.add_argument("--host", default="0.0.0.0")
    b.add_argument("--port", type=int, default=9090)
    b.add_argument("--data-dir", default="./data")
    b.add_argument("--max-segment-bytes", type=int, default=1048576)
    b.add_argument("--node-id", default="node-0")
    b.add_argument("--role", default="leader", choices=["leader", "follower"])
    b.add_argument("--peer-host", default=None, help="Follower/leader peer host")
    b.add_argument("--peer-port", type=int, default=9091)
    b.add_argument("--heartbeat-interval", type=float, default=2.0)
    b.add_argument("--election-timeout", type=float, default=6.0)
    b.add_argument("--auto-failover", action="store_true", default=True)
    b.add_argument("--no-auto-failover", action="store_false", dest="auto_failover")
    b.add_argument("--retention-ms", type=int, default=None,
                   help="Delete segments older than N ms (default: keep forever)")

    # --- produce ---
    p = sub.add_parser("produce", help="Send messages to a topic")
    p.add_argument("topic")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9090)
    p.add_argument("--key", default=None)
    p.add_argument("--message", "-m", default=None, help="Single message to send")
    p.add_argument("--file", "-f", default=None, help="File with one message per line")

    # --- consume ---
    c = sub.add_parser("consume", help="Consume messages from a topic")
    c.add_argument("topic")
    c.add_argument("--host", default="127.0.0.1")
    c.add_argument("--port", type=int, default=9090)
    c.add_argument("--group", default="default")
    c.add_argument("--max-bytes", type=int, default=65536)
    c.add_argument("--poll-interval", type=float, default=0.5)

    args = parser.parse_args()
    if args.command == "broker":
        run_broker(args)
    elif args.command == "produce":
        run_producer(args)
    elif args.command == "consume":
        run_consumer(args)


if __name__ == "__main__":
    main()
