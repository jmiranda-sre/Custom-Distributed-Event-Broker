# Custom Distributed Event Broker

A minimalist distributed event broker built from scratch in Python, demonstrating the core internals of systems like Kafka and RabbitMQ. Zero external dependencies — only the Python 3.11+ standard library.

## Architecture

```
┌──────────────┐      ┌─────────────────────────────────────┐      ┌──────────────┐
│   Producer   │────▶│          Broker (Leader)            │────▶│   Consumer   │
│   Client     │ TCP  │                                     │ TCP  │   Client     │
└──────────────┘      │  ┌──────────┐    ┌───────────────┐  │      └──────────────┘
                      │  │Command   │    │ CommitLog     │  │
┌──────────────┐      │  │Dispatcher│──▶│ (append-only) │  │      ┌──────────────┐
│   Consumer   │────▶│  └────┬─────┘    └───────┬───────┘  │────▶│   Producer   │
│   Client     │      │       │                  │          │      │   Client     │
└──────────────┘      │  ┌────▼────┐      ┌──────▼───────┐  │      └──────────────┘
                      │  │Offset   │      │  Segment     │  │
                      │  │Manager  │      │  .log + .idx │  │
                      │  └─────────┘      └──────────────┘  │
                      │                                     │
                      │  ┌───────────────────────────────┐  │
                      │  │     Failover Module           │  │
                      │  │  Heartbeat ──▶ Follower      │  │
                      │  │  Replication (REPLICATE)      │  │
                      │  └───────────────────────────────┘  │
                      └─────────────────────────────────────┘
                                    │
                                    │ TCP (REPLICATE frames)
                                    ▼
                     ┌───────────────────────────────────┐
                     │       Broker (Follower)           │
                     │  Same structure, read-only until  │
                     │  promoted to leader               │
                     └───────────────────────────────────┘
```

### Core Components

| Module | Path | Description |
|---|---|---|
| **Commit Log** | `deb/log/commit_log.py` | Binary append-only log with segmentation, sparse index, CRC validation |
| **Wire Protocol** | `deb/protocol/frame.py` | Binary TCP frame protocol: `[4B len][1B cmd][JSON payload]` |
| **Broker Server** | `deb/broker/server.py` | Async TCP server handling PRODUCE/FETCH/ACK/SUBSCRIBE |
| **Offset Manager** | `deb/broker/offset_manager.py` | Consumer group offset tracking with JSONL persistence |
| **Failover Module** | `deb/failover/failover.py` | Leader/follower heartbeat, auto-promotion, log replication |
| **Producer** | `deb/client/producer.py` | Async producer client |
| **Consumer** | `deb/client/consumer.py` | Async consumer with at-least-once delivery |
| **CLI** | `deb/cli.py` | Command-line entry points |

## Key Design Decisions

### 1. Append-Only Commit Log

Messages are written to binary `.log` files using `pwrite()` for position-independent writes. Each record format:

```
┌──────────┬──────────┬──────────┬──────────┬───────┬─────────┐
│ 8B       │ 4B       │ 4B       │ 4B       │ kB    │ vB      │
│ offset   │ crc32    │ key_len  │ val_len  │ key   │ value   │
└──────────┴──────────┴──────────┴──────────┴───────┴─────────┘
```

A companion `.idx` file maps offset → file position for O(1) lookups:

```
┌──────────┬──────────┐
│ 8B       │ 4B       │
│ offset   │ position │
└──────────┴──────────┘
```

**Segmentation**: When a segment exceeds `max_segment_bytes`, a new segment is created. This enables efficient log cleanup and prevents unbounded file growth. On recovery, the index is rebuilt from the log file if the `.idx` is missing or corrupt.

**Retention**: When `--retention-ms` is set, the broker periodically scans old segments and deletes those whose last modification time exceeds the retention window. The active (newest) segment is never deleted. This prevents unbounded disk usage in long-running deployments.

**pread/pwrite**: All reads use `os.pread()` (position-independent) to avoid file offset corruption when multiple FDs reference the same file across `asyncio.run()` calls in the same process.

### 2. At-Least-Once Delivery

```
Consumer                    Broker
   │                          │
   │── SUBSCRIBE(topic,grp) ─▶│  Returns last committed offset
   │                          │
   │── FETCH(offset, max) ───▶│  Returns batch of messages
   │◀── MESSAGE_BATCH ────────│
   │                          │
   │   ... process messages ..│
   │                          │
   │── ACK(topic, grp, off) ─▶│  Commits offset to disk
   │                          │
```

If the consumer crashes before sending ACK, the next FETCH starts from the last committed offset → **redelivery** of uncommitted messages. This is the at-least-once guarantee: messages may be delivered more than once, but never lost.

### 3. Wire Protocol

All communication uses a simple binary frame format over TCP:

```
┌──────────────┬─────────────┬────────────────────┐
│ 4B           │ 1B          │ N bytes            │
│ payload_len  │ command     │ JSON-encoded body  │
└──────────────┴─────────────┴────────────────────┘
```

Commands: `PRODUCE(0x01)`, `SUBSCRIBE(0x03)`, `FETCH(0x04)`, `ACK(0x06)`, `HEARTBEAT(0x07)`, `REPLICATE(0x08)`, `LEADER_ANNOUNCE(0x0A)`, etc.

### 4. Failover (Leader/Follower)

Two-node cluster with semi-automatic failover:

- **Leader** sends `LEADER_ANNOUNCE` heartbeats to follower every `heartbeat_interval`
- **Leader** replicates all `PRODUCE` entries via `REPLICATE` frames to follower
- **Follower** monitors heartbeats; if none received for `election_timeout`, promotes itself
- Promotion increments the `term` (similar to Raft)
- `auto_failover=True` → automatic promotion; `False` → requires manual `--force` flag

## Quick Start

### Install (dev mode)

```bash
pip install -e .
```

Or run directly without installation:

```bash
python3 -m deb.cli <command>
```

### Start a Broker

```bash
# Single-node broker (default port 9090)
python3 -m deb.cli broker --port 9090 --data-dir ./data

# Leader node with failover
python3 -m deb.cli broker --port 9090 --role leader --node-id node-0 \
    --peer-host 127.0.0.1 --peer-port 9091 --data-dir ./data-leader

# Follower node with failover
python3 -m deb.cli broker --port 9091 --role follower --node-id node-1 \
    --peer-host 127.0.0.1 --peer-port 9090 --data-dir ./data-follower
```

### Produce Messages

```bash
# Send a single message
python3 -m deb.cli produce my-topic -m "Hello, World!"

# Interactive mode (type messages, Ctrl+D to exit)
python3 -m deb.cli produce my-topic

# Send from a file (one message per line)
python3 -m deb.cli produce my-topic -f messages.txt

# With a message key
python3 -m deb.cli produce my-topic -m "event" --key "user-123"
```

### Consume Messages

```bash
# Start a consumer (default group: "default")
python3 -m deb.cli consume my-topic

# With a custom consumer group
python3 -m deb.cli consume my-topic --group analytics

# Custom poll interval
python3 -m deb.cli consume my-topic --poll-interval 1.0
```

### Run Tests

```bash
python3 -m unittest tests.test_e2e -v
```

## Configuration Options

### Broker

| Flag | Default | Description |
|---|---|---|
| `--host` | `0.0.0.0` | Listen address |
| `--port` | `9090` | Listen port |
| `--data-dir` | `./data` | Storage directory for logs and offsets |
| `--max-segment-bytes` | `1048576` (1MB) | Max size per log segment before rolling |
| `--node-id` | `node-0` | Unique node identifier |
| `--role` | `leader` | `leader` or `follower` |
| `--peer-host` | None | Failover peer host |
| `--peer-port` | `9091` | Failover peer port |
| `--heartbeat-interval` | `2.0` | Seconds between heartbeats |
| `--election-timeout` | `6.0` | Seconds before follower promotes |
| `--auto-failover` | `True` | Auto-promote on leader timeout |
| `--retention-ms` | None | Delete segments older than N ms (default: keep forever) |

### Producer

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | Broker address |
| `--port` | `9090` | Broker port |
| `--key` | None | Optional message key |
| `-m` / `--message` | None | Single message to send |
| `-f` / `--file` | None | File with one message per line |

### Consumer

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | Broker address |
| `--port` | `9090` | Broker port |
| `--group` | `default` | Consumer group name |
| `--max-bytes` | `65536` | Max bytes per FETCH request |
| `--poll-interval` | `0.5` | Seconds between empty-fetch polls |

## How It Works

### Message Lifecycle

1. **Producer** sends `PRODUCE(topic, key, value)` via TCP
2. **Broker** appends to the topic's `CommitLog` → returns assigned offset
3. **Broker** replicates to follower (if configured)
4. **Consumer** sends `SUBSCRIBE(topic, group)` → gets starting offset
5. **Consumer** sends `FETCH(offset, max_bytes)` → receives `MESSAGE_BATCH`
6. **Consumer** processes messages, sends `ACK(topic, group, last_offset)`
7. **Broker** persists committed offset to `consumer_offsets.jsonl`

### Recovery After Restart

- **Log data**: Automatically recovered from `.log` files using `.idx` for fast lookup. If `.idx` is missing/corrupt, a full log scan rebuilds it.
- **Consumer offsets**: Loaded from `consumer_offsets.jsonl` (compacted on clean shutdown).
- **Segment continuity**: New segments start at the next offset after the last recovered record.

### Failover Scenario

```
Time ─────────────────────────────────────────────────▶

Leader:   ── heartbeat ── heartbeat ── ✗ CRASH
                                     │
Follower: ── replicate ── replicate ── timeout ── PROMOTE ── LEADER
                                     │          (term+1)
Clients:  ── produce  ── produce   ── retry  ── produce (to new leader)
```

## Implementation Notes

- **Zero dependencies**: Only Python 3.11+ stdlib (`asyncio`, `struct`, `zlib`, `os`, `json`)
- **`pread`/`pwrite`**: Used instead of `lseek`+`read`/`write` to avoid file position corruption across `asyncio.run()` calls
- **`O_CLOEXEC`**: All file descriptors created with `O_CLOEXEC` to prevent fd leaks on exec
- **CRC32 validation**: Each record includes a CRC32 checksum for data integrity
- **Thread-safe offset manager**: Uses `threading.Lock` for concurrent offset commits
- **Connection keep-alive**: Heartbeat frames with configurable timeout; idle connections probed every `3 × heartbeat_interval`

## Project Structure

```
CustomDistributedEventBroker/
├── deb/
│   ├── __init__.py
│   ├── cli.py                    # CLI entry points (broker/produce/consume)
│   ├── log/
│   │   ├── __init__.py
│   │   └── commit_log.py         # Append-only log + segmentation + index
│   ├── protocol/
│   │   ├── __init__.py
│   │   └── frame.py              # Binary wire protocol (TCP frames)
│   ├── broker/
│   │   ├── __init__.py
│   │   ├── server.py             # Async TCP broker server
│   │   └── offset_manager.py     # Consumer group offset tracking
│   ├── failover/
│   │   ├── __init__.py
│   │   └── failover.py           # Leader/follower failover module
│   └── client/
│       ├── __init__.py
│       ├── producer.py           # Async producer client
│       └── consumer.py           # Async consumer (at-least-once)
├── tests/
│   ├── __init__.py
│ └── test_e2e.py # End-to-end test suite (20 tests)
└── pyproject.toml
```

## License

MIT
