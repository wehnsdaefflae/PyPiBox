# coding=utf-8
from __future__ import annotations

import enum
import hashlib
import pathlib
from dataclasses import dataclass
from typing import Optional, Union

from dropbox import files


class SyncDirection(str, enum.Enum):
    UP = "up"
    DOWN = "down"


class SyncAction(str, enum.Enum):
    ADD = "add"
    DEL = "del"


class FileInfo:
    def __init__(self, path: pathlib.PurePosixPath, timestamp: float, is_folder: bool, dropbox_hash: Optional[str] = None):
        self.path = path
        self.timestamp = timestamp
        self.is_folder = is_folder
        self.dropbox_hash = dropbox_hash

    @property
    def actual(self) -> pathlib.Path:
        return pathlib.Path(self.path)

    @property
    def is_deleted(self) -> bool:
        return self.dropbox_hash is None and not self.is_folder

    def __eq__(self, other: FileInfo) -> bool:
        if not isinstance(other, FileInfo):
            return False

        return self.path == other.path and self.dropbox_hash == other.dropbox_hash

    def __hash__(self) -> int:
        return hash((self.path, self.dropbox_hash))

    def __repr__(self):
        return f"FileInfo(path={self.path.as_posix():s}, timestamp={self.timestamp:.2f}, is_folder={self.is_folder!s:s}, hash={self.dropbox_hash:s})"

    def __str__(self):
        return self.__repr__()


FILE_INDEX = dict[pathlib.PurePosixPath, FileInfo]


@dataclass
class Delta:
    modified: FILE_INDEX
    deleted: FILE_INDEX


def compute_dropbox_hash(file_path: pathlib.Path) -> str:
    # https://stackoverflow.com/questions/13008040/locally-calculate-dropbox-hash-of-files
    dropbox_hash_chunk_size = 4 * 1024 * 1024
    block_hashes = b''

    with file_path.open(mode="rb") as f:
        while chunk := f.read(dropbox_hash_chunk_size):
            chunk_hash = hashlib.sha256(chunk)
            block_hashes += chunk_hash.digest()

    total_hash = hashlib.sha256(block_hashes)
    return total_hash.hexdigest()


def get_mod_time_locally(file_path: pathlib.Path) -> float:
    """Returns the modification time of a file in seconds since the epoch."""
    stat = file_path.stat()
    timestamp = stat.st_mtime
    return round(timestamp, 1)


def get_mod_time_remotely(entry: Union[files.FileMetadata, files.FolderMetadata], offset: float = 2 * 60 * 60) -> float:
    stat = entry.server_modified
    return stat.timestamp() + offset


def depth(file_path: pathlib.PurePath) -> int:
    path_string = file_path.as_posix()
    return path_string.count("/")
