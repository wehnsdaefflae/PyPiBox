from __future__ import annotations

import datetime
import hashlib
import os
from dataclasses import dataclass
from typing import Optional, Union

from dateutil import tz
from dropbox import files


@dataclass
class PathInfo:
    path: str
    timestamp: float
    hash: Optional[str]

    @property
    def is_folder(self) -> bool:
        return self.path.endswith("/")

    def __eq__(self, other: PathInfo) -> bool:
        if not isinstance(other, PathInfo):
            return False
        return self.path == other.path and self.timestamp == other.timestamp and self.hash == other.hash

    def __hash__(self) -> int:
        return hash((self.path, self.hash, self.timestamp))


def compute_dropbox_hash(path: str) -> str:
    # https://stackoverflow.com/questions/13008040/locally-calculate-dropbox-hash-of-files
    dropbox_hash_chunk_size = 4 * 1024 * 1024
    block_hashes = b''

    with open(path, mode="rb") as f:
        while chunk := f.read(dropbox_hash_chunk_size):
            chunk_hash = hashlib.sha256(chunk)
            block_hashes += chunk_hash.digest()

    total_hash = hashlib.sha256(block_hashes)
    return total_hash.hexdigest()


def get_modification_timestamp_locally(file_path: str) -> float:
    stat = os.stat(file_path)
    timestamp = stat.st_mtime
    dt_local = datetime.datetime.fromtimestamp(timestamp, tz=tz.tzlocal())
    dt = dt_local.astimezone(tz=tz.tzutc())
    return round(dt.timestamp(), 1)


def get_modification_timestamp_remotely(entry: Union[files.FileMetadata, files.FolderMetadata]) -> float:
    stat = entry.server_modified
    return stat.timestamp()


def depth(path: PathInfo) -> int:
    return path.path.count("/")
