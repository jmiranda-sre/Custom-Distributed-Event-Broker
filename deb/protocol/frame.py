"""Binary wire protocol for TCP communication.

Frame format (little-endian):
  [4B payload_len][1B command][payload]

Command codes:
  0x01 PRODUCE         — producer → broker: publish message
  0x02 PRODUCE_ACK     — broker → producer: publish confirmation
  0x03 SUBSCRIBE       — consumer → broker: register consumer group + topic
  0x04 FETCH           — consumer → broker: request messages from offset
  0x05 MESSAGE_BATCH   — broker → consumer: batch of messages
  0x06 ACK             — consumer → broker: confirm processed offset
  0x07 HEARTBEAT       — bidirectional: keep-alive
  0x08 REPLICATE       — leader → follower: replicate log entry
  0x09 REPLICATE_ACK   — follower → leader: replication confirmation
  0x0A LEADER_ANNOUNCE — broker → cluster: leader identity broadcast
  0x0B OFFSET_REPLY    — broker → consumer: reply with current end offset
  0x0C ERROR           — broker → client: error response

Payload formats (JSON-encoded bytes for simplicity, minimal overhead):
  PRODUCE:   {"topic": str, "key": str|None, "value": str}
  PRODUCE_ACK: {"offset": int, "topic": str}
  SUBSCRIBE: {"topic": str, "group": str}
  FETCH:     {"topic": str, "group": str, "offset": int, "max_bytes": int}
  MESSAGE_BATCH: {"topic": str, "messages": [{"offset":int,"key":str|None,"value":str},...]}
  ACK:       {"topic": str, "group": str, "offset": int}
  HEARTBEAT: {"ts": float}
  REPLICATE: {"topic": str, "entry": {"offset":int,"crc32":int,"key":str|None,"value":str}}
  REPLICATE_ACK: {"topic": str, "offset": int}
  LEADER_ANNOUNCE: {"leader_id": str, "term": int}
  OFFSET_REPLY: {"topic": str, "group": str, "offset": int}
  ERROR:     {"code": int, "message": str}
"""

import json
import struct
from enum import IntEnum
from typing import Any

HEADER_FMT = "<IB"        # little-endian: uint32 payload_len, uint8 cmd
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 5 bytes


class Cmd(IntEnum):
    PRODUCE         = 0x01
    PRODUCE_ACK     = 0x02
    SUBSCRIBE       = 0x03
    FETCH           = 0x04
    MESSAGE_BATCH   = 0x05
    ACK             = 0x06
    HEARTBEAT       = 0x07
    REPLICATE       = 0x08
    REPLICATE_ACK   = 0x09
    LEADER_ANNOUNCE = 0x0A
    OFFSET_REPLY    = 0x0B
    ERROR           = 0x0C


class Frame:
    __slots__ = ("cmd", "payload")

    def __init__(self, cmd: Cmd, payload: dict[str, Any]):
        self.cmd = cmd
        self.payload = payload

    def encode(self) -> bytes:
        body = json.dumps(self.payload, separators=(",", ":")).encode()
        return struct.pack(HEADER_FMT, len(body), self.cmd) + body

    @staticmethod
    async def read_from(stream: Any) -> "Frame":
        """Read one frame from asyncio StreamReader."""
        header = await _read_exact(stream, HEADER_SIZE)
        payload_len, cmd_byte = struct.unpack(HEADER_FMT, header)
        cmd = Cmd(cmd_byte)
        body = await _read_exact(stream, payload_len) if payload_len else b""
        payload = json.loads(body) if body else {}
        return Frame(cmd, payload)

    def __repr__(self) -> str:
        return f"Frame(cmd={self.cmd.name}, payload={self.payload})"


async def _read_exact(stream: Any, n: int) -> bytes:
    """Read exactly n bytes from stream, raising ConnectionError on EOF."""
    data = b""
    while len(data) < n:
        chunk = await stream.read(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed while reading frame")
        data += chunk
    return data
