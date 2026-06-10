"""Consumer group offset tracking — persisted to disk.

Format per line (JSON): {"topic":str, "group":str, "offset":int}
File: <data_dir>/consumer_offsets.jsonl
"""

import json
from pathlib import Path
from typing import Dict, Tuple
import threading


class OffsetManager:
    """Thread-safe consumer offset tracking with disk persistence."""

    def __init__(self, data_dir: str):
        self._path = Path(data_dir) / "consumer_offsets.jsonl"
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._offsets: Dict[Tuple[str, str], int] = {}  # (topic, group) → offset
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if not self._path.exists():
            return
        with open(self._path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    key = (rec["topic"], rec["group"])
                    # keep highest offset per key (log is append-only)
                    existing = self._offsets.get(key, -1)
                    if rec["offset"] > existing:
                        self._offsets[key] = rec["offset"]
                except (json.JSONDecodeError, KeyError):
                    continue

    def get_last_committed(self, topic: str, group: str) -> int:
        """Return the last committed offset (-1 if never committed)."""
        with self._lock:
            return self._offsets.get((topic, group), -1)

    def get_next_offset(self, topic: str, group: str) -> int:
        """Return the next offset to consume (last_committed + 1, or 0)."""
        with self._lock:
            last = self._offsets.get((topic, group), -1)
            return last + 1 if last >= 0 else 0

    def commit(self, topic: str, group: str, offset: int):
        with self._lock:
            current = self._offsets.get((topic, group), -1)
            if offset <= current:
                return  # stale commit, skip
            self._offsets[(topic, group)] = offset
            self._append_to_disk(topic, group, offset)

    def _append_to_disk(self, topic: str, group: str, offset: int):
        rec = json.dumps({"topic": topic, "group": group, "offset": offset}, separators=(",",":"))
        with open(self._path, "a") as f:
            f.write(rec + "\n")
            f.flush()
            os_fsync(f)

    def close(self):
        """Compact the offsets file on shutdown."""
        with self._lock:
            with open(self._path, "w") as f:
                for (topic, group), offset in sorted(self._offsets.items()):
                    rec = json.dumps({"topic": topic, "group": group, "offset": offset}, separators=(",",":"))
                    f.write(rec + "\n")
                f.flush()
                os_fsync(f)


def os_fsync(f):
    try:
        import os
        os.fsync(f.fileno())
    except OSError:
        pass
