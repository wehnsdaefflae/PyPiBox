from __future__ import annotations

import enum
import hashlib
import os
from dataclasses import dataclass
from typing import Optional, Union

from dropbox import files


class SyncDirection(str, enum.Enum):
    UP = "up"
    DOWN = "down"


class SyncAction(str, enum.Enum):
    ADD = "add"
    DEL = "del"


@dataclass
class FileInfo:
    path: str
    timestamp: float
    hash: Optional[str]

    @property
    def is_folder(self) -> bool:
        return self.path.endswith("/")

    @property
    def is_deleted(self) -> bool:
        return self.hash is None and not self.is_folder

    def __eq__(self, other: FileInfo) -> bool:
        if not isinstance(other, FileInfo):
            return False

        return self.path == other.path and self.hash == other.hash

    def __hash__(self) -> int:
        return hash((self.path, self.hash))


FILE_INDEX = dict[str, FileInfo]


@dataclass
class Delta:
    modified: FILE_INDEX
    deleted: FILE_INDEX


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


def get_mod_time_locally(file_path: str) -> float:
    stat = os.stat(file_path)
    timestamp = stat.st_mtime
    return round(timestamp, 1)


def get_mod_time_remotely(entry: Union[files.FileMetadata, files.FolderMetadata]) -> float:
    stat = entry.server_modified
    return stat.timestamp() + 2 * 60 * 60


def depth(path_str: str) -> int:
    return path_str.count("/")
