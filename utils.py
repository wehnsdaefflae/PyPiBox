# coding=utf-8
from __future__ import annotations

import enum
import hashlib
import pathlib
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Optional, Union

from dropbox import files


class SyncDirection(str, enum.Enum):
    UP = "up"
    DOWN = "down"


class SyncAction(str, enum.Enum):
    ADD = "add"
    DEL = "del"


class FileInfo(ABC):
    def __init__(self, path: pathlib.PurePosixPath, is_folder: bool):
        self.path = path
        self.is_folder = is_folder

    @abstractmethod
    def get_dropbox_hash(self) -> Optional[str]:
        pass

    @abstractmethod
    def get_size(self) -> int:
        pass

    @abstractmethod
    def get_modified_timestamp(self) -> float:
        pass

    def __eq__(self, other: FileInfo) -> bool:
        if not isinstance(other, LocalFile):
            return False

        return self.path.as_posix() == other.actual.as_posix() and self.get_modified_timestamp() == other.get_modified_timestamp()

    def __hash__(self) -> int:
        return hash((self.path.as_posix(), self.get_modified_timestamp(), self.get_size()))

    def __repr__(self):
        return f"FileInfo(path={self.path.as_posix():s}, is_folder={self.is_folder!s:s})"

    def __str__(self):
        return self.__repr__()


class LocalFile(FileInfo):
    def __init__(self, path: pathlib.PosixPath):
        super().__init__(pathlib.PurePosixPath(path.as_posix()), path.is_dir())
        self.actual = path
        self.dropbox_hash = None
        self.size = -1
        self.timestamp = -1.

    def get_dropbox_hash(self) -> Optional[str]:
        if self.is_folder:
            return None

        if self.dropbox_hash is None:
            self.dropbox_hash = compute_dropbox_hash(self.actual)

        return self.dropbox_hash

    def get_size(self) -> int:
        if self.size < 0:
            stat = self.actual.stat()
            self.size = stat.st_size
        return self.size

    def get_modified_timestamp(self) -> float:
        if self.timestamp < 0.:
            self.timestamp = get_mod_time_locally(self.actual)

        return self.timestamp


class RemoteFile(FileInfo):
    def get_dropbox_hash(self) -> Optional[str]:
        return self.entry.content_hash

    def get_size(self) -> int:
        return self.entry.size

    def get_modified_timestamp(self) -> float:
        if isinstance(self.entry, files.FileMetadata):
            return self.entry.client_modified.timestamp()
        return 0.

    def __init__(self, entry: Union[files.FileMetadata, files.FolderMetadata]):
        super().__init__(pathlib.PurePosixPath(entry.path_lower), isinstance(entry, files.FolderMetadata))
        self.entry = entry


LOCAL_FILE_INDEX = dict[pathlib.PurePosixPath, LocalFile]
REMOTE_FILE_INDEX = dict[pathlib.PurePosixPath, RemoteFile]
FILE_INDEX = Union[LOCAL_FILE_INDEX, REMOTE_FILE_INDEX]


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


def get_size_locally(file_path: pathlib.Path) -> int:
    """Returns the size of a file in bytes."""
    stat = file_path.stat()
    return stat.st_size


def get_mod_time_remotely(entry: Union[files.FileMetadata, files.FolderMetadata]) -> float:
    stat = entry.client_modified
    return stat.timestamp()


def depth(file_path: pathlib.PurePath) -> int:
    path_string = file_path.as_posix()
    return path_string.count("/")
