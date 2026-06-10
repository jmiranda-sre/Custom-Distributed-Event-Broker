"""Append-only commit log with segmentation.

Binary record format (little-endian):
  [8B offset][4B crc32][4B key_len][4B val_len][key_bytes][value_bytes]

Index format (little-endian):
  [8B offset][4B file_position]

Segment naming: {base_offset:020d}.log  /  {base_offset:020d}.idx
When a segment exceeds max_bytes, a new one is created starting at the next offset.
"""

import struct
import os
import zlib
import json
from pathlib import Path
from typing import Optional

RECORD_HEADER_FMT = "<QII I"   # offset(u64), crc32(u32), key_len(u32), val_len(u32)
RECORD_HEADER_SIZE = struct.calcsize(RECORD_HEADER_FMT)  # 24 bytes
INDEX_ENTRY_FMT = "<QI"        # offset(u64), file_pos(u32)
INDEX_ENTRY_SIZE = struct.calcsize(INDEX_ENTRY_FMT)  # 12 bytes


class LogEntry:
    __slots__ = ("offset", "crc32", "key", "value")

    def __init__(self, offset: int, crc32: int, key: Optional[bytes], value: bytes):
        self.offset = offset
        self.crc32 = crc32
        self.key = key
        self.value = value

    def to_dict(self) -> dict:
        return {
            "offset": self.offset,
            "crc32": self.crc32,
            "key": self.key.decode() if self.key else None,
            "value": self.value.decode(),
        }


class Segment:
    """Single log segment file pair (.log + .idx)."""

    def __init__(self, log_dir: Path, base_offset: int, max_bytes: int):
        self.log_dir = log_dir
        self.base_offset = base_offset
        self.max_bytes = max_bytes
        self._log_path = log_dir / f"{base_offset:020d}.log"
        self._idx_path = log_dir / f"{base_offset:020d}.idx"
        self._log_fd: Optional[int] = None
        self._idx_fd: Optional[int] = None
        self._log_size: int = 0
        self._next_offset: int = base_offset
        self._index: dict[int, int] = {}  # offset → file_pos
        self._open()

    def _open(self):
        flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC
        self._log_fd = os.open(str(self._log_path), flags, 0o644)
        self._idx_fd = os.open(str(self._idx_path), flags, 0o644)
        if self._log_path.stat().st_size > 0:
            self._log_size = self._log_path.stat().st_size
            self._recover_index()
            # _recover_index already sets _next_offset
            if self._next_offset == self.base_offset and self._log_size > 0:
                # index was empty/corrupt — full scan
                self._rebuild_index_from_log()
        else:
            self._log_size = 0
            self._next_offset = self.base_offset

    def _recover_index(self):
        """Load existing index from file."""
        self._index.clear()
        self._next_offset = self.base_offset
        if not self._idx_path.exists():
            return
        idx_data = self._idx_path.read_bytes()
        for i in range(0, len(idx_data), INDEX_ENTRY_SIZE):
            entry = idx_data[i:i + INDEX_ENTRY_SIZE]
            if len(entry) < INDEX_ENTRY_SIZE:
                break
            offset, pos = struct.unpack(INDEX_ENTRY_FMT, entry)
            self._index[offset] = pos
            self._next_offset = offset + 1

    def _rebuild_index_from_log(self):
        """Full scan of log file to rebuild index (recovery path)."""
        self._index.clear()
        self._next_offset = self.base_offset
        if not self._log_path.exists():
            return
        data = self._log_path.read_bytes()
        pos = 0
        while pos + RECORD_HEADER_SIZE <= len(data):
            header = data[pos:pos + RECORD_HEADER_SIZE]
            offset, crc, klen, vlen = struct.unpack(RECORD_HEADER_FMT, header)
            rec_end = pos + RECORD_HEADER_SIZE + klen + vlen
            if rec_end > len(data):
                break  # truncated record — stop
            self._index[offset] = pos
            self._next_offset = offset + 1
            pos = rec_end
        self._log_size = pos
        # rewrite index
        self._write_index()

    def _write_index(self):
        """Write entire index to disk."""
        os.close(self._idx_fd) if self._idx_fd is not None else None
        self._idx_fd = os.open(str(self._idx_path), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
        buf = b""
        for offset in sorted(self._index):
            buf += struct.pack(INDEX_ENTRY_FMT, offset, self._index[offset])
        if buf:
            os.write(self._idx_fd, buf)
        os.fsync(self._idx_fd)

    @property
    def is_full(self) -> bool:
        return self._log_size >= self.max_bytes

    @property
    def next_offset(self) -> int:
        return self._next_offset

    def append(self, key: Optional[bytes], value: bytes) -> LogEntry:
        """Append a record. Returns the LogEntry with assigned offset and crc32."""
        if self._log_fd is None or self._idx_fd is None:
            raise RuntimeError("Segment is closed")
        offset = self._next_offset
        klen = len(key) if key else 0
        vlen = len(value)
        # CRC over key + value
        crc = zlib.crc32((key or b"") + value) & 0xFFFFFFFF
        header = struct.pack(RECORD_HEADER_FMT, offset, crc, klen, vlen)
        record = header + (key or b"") + value
        n = os.pwrite(self._log_fd, record, self._log_size)
        os.fsync(self._log_fd)
        # update index
        self._index[offset] = self._log_size
        idx_entry = struct.pack(INDEX_ENTRY_FMT, offset, self._log_size)
        os.write(self._idx_fd, idx_entry)
        os.fsync(self._idx_fd)
        self._log_size += n
        self._next_offset = offset + 1
        return LogEntry(offset, crc, key, value)

    def read(self, offset: int) -> Optional[LogEntry]:
        """Read a single record by offset using index."""
        pos = self._index.get(offset)
        if pos is None:
            return None
        return self._read_at(pos)

    def read_range(self, start_offset: int, max_bytes: int) -> list[LogEntry]:
        """Read records from start_offset up to max_bytes total."""
        entries: list[LogEntry] = []
        total = 0
        off = start_offset
        while off < self._next_offset:
            pos = self._index.get(off)
            if pos is None:
                break
            entry = self._read_at(pos)
            if entry is None:
                break
            rec_size = RECORD_HEADER_SIZE + (len(entry.key) if entry.key else 0) + len(entry.value)
            if total + rec_size > max_bytes and total > 0:
                break
            entries.append(entry)
            total += rec_size
            off += 1
        return entries

    def _read_at(self, pos: int) -> Optional[LogEntry]:
        """Read record at absolute file position using pread (no shared file offset)."""
        if self._log_fd is None:
            return None
        try:
            header = os.pread(self._log_fd, RECORD_HEADER_SIZE, pos)
            if len(header) < RECORD_HEADER_SIZE:
                return None
            offset, crc, klen, vlen = struct.unpack(RECORD_HEADER_FMT, header)
            key_pos = pos + RECORD_HEADER_SIZE
            key = os.pread(self._log_fd, klen, key_pos) if klen else None
            val_pos = key_pos + klen
            value = os.pread(self._log_fd, vlen, val_pos)
            if len(value) < vlen:
                return None  # truncated
            return LogEntry(offset, crc, key, value)
        except OSError:
            return None

    def close(self):
        if self._log_fd is not None:
            try:
                os.close(self._log_fd)
            except OSError:
                pass
            self._log_fd = None
        if self._idx_fd is not None:
            try:
                os.close(self._idx_fd)
            except OSError:
                pass
            self._idx_fd = None


class CommitLog:
    """Multi-segment append-only commit log for a single topic.

    Supports time-based retention: segments whose newest record is older
    than ``retention_ms`` are deleted on ``cleanup()``.  The active segment
    is never deleted.  Pass ``retention_ms=None`` (default) to disable
    retention — segments live forever.
    """

    def __init__(
        self,
        log_dir: str,
        topic: str,
        max_segment_bytes: int = 1024 * 1024,
        retention_ms: Optional[int] = None,
    ):
        self._dir = Path(log_dir) / topic
        self._dir.mkdir(parents=True, exist_ok=True)
        self._max_segment_bytes = max_segment_bytes
        self._retention_ms = retention_ms
        self._segments: list[Segment] = []
        self._active_segment: Optional[Segment] = None
        self._load_segments()

    def _load_segments(self):
        """Load or create segments on startup."""
        log_files = sorted(self._dir.glob("*.log"))
        if log_files:
            for lf in log_files:
                base = int(lf.stem)
                seg = Segment(self._dir, base, self._max_segment_bytes)
                self._segments.append(seg)
            self._active_segment = self._segments[-1]
            if self._active_segment.is_full:
                self._roll_segment()
        else:
            self._roll_segment()

    def _roll_segment(self):
        """Create a new active segment."""
        base = self._active_segment.next_offset if self._active_segment else 0
        seg = Segment(self._dir, base, self._max_segment_bytes)
        self._segments.append(seg)
        self._active_segment = seg

    def append(self, key: Optional[bytes], value: bytes) -> LogEntry:
        """Append message to active segment; roll if full."""
        if self._active_segment.is_full:
            self._roll_segment()
        return self._active_segment.append(key, value)

    def read(self, offset: int) -> Optional[LogEntry]:
        """Read a single entry by offset, searching segments."""
        for seg in self._segments:
            if seg.base_offset <= offset < seg.next_offset:
                return seg.read(offset)
        return None

    def read_range(self, start_offset: int, max_bytes: int = 65536) -> list[LogEntry]:
        """Read entries from start_offset up to max_bytes."""
        for i, seg in enumerate(self._segments):
            if seg.base_offset <= start_offset < seg.next_offset:
                entries = seg.read_range(start_offset, max_bytes)
                # continue into next segments if budget allows
                budget = max_bytes
                total = sum(
                    RECORD_HEADER_SIZE + (len(e.key) if e.key else 0) + len(e.value)
                    for e in entries
                )
                budget -= total
                j = i + 1
                while budget > 0 and j < len(self._segments):
                    more = self._segments[j].read_range(
                        self._segments[j].base_offset, budget
                    )
                    if not more:
                        break
                    entries.extend(more)
                    budget -= sum(
                        RECORD_HEADER_SIZE + (len(e.key) if e.key else 0) + len(e.value)
                        for e in more
                    )
                    j += 1
                return entries
        # offset beyond all segments — return empty
        return []

    @property
    def next_offset(self) -> int:
        return self._active_segment.next_offset if self._active_segment else 0

    def cleanup(self) -> int:
        """Delete old segments whose mtime exceeds retention_ms.

        The active (last) segment is never deleted.  Returns the number
        of segments removed.
        """
        if self._retention_ms is None or len(self._segments) <= 1:
            return 0
        import time

        cutoff = time.time() * 1000 - self._retention_ms
        removed = 0
        while len(self._segments) > 1:
            seg = self._segments[0]
            # Use the index file mtime as proxy for "last write time".
            # If index missing, fall back to log file mtime.
            probe = seg._idx_path if seg._idx_path.exists() else seg._log_path
            mtime_ms = probe.stat().st_mtime * 1000
            if mtime_ms >= cutoff:
                break
            seg.close()
            # Remove files from disk
            for p in (seg._log_path, seg._idx_path):
                if p.exists():
                    p.unlink()
            self._segments.pop(0)
            removed += 1
        return removed

    def close(self):
        for seg in self._segments:
            seg.close()
