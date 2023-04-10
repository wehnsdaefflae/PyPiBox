# coding=utf-8
from __future__ import annotations

import enum
import hashlib
import pathlib
from abc import abstractmethod, ABC
from typing import Optional, Union

from dropbox import files


class SyncDirection(str, enum.Enum):
    UP = "up"
    DOWN = "down"


class SyncAction(str, enum.Enum):
    ADD = "add"
    DEL = "del"


class FileInfo(ABC):
    def __init__(self, relative_path: pathlib.PurePosixPath, is_folder: bool):
        self.relative_path = relative_path
        self.is_folder = is_folder
        self.posix_path = relative_path.as_posix()

    @abstractmethod
    def _get_dropbox_hash(self) -> str:
        pass

    def get_dropbox_hash(self) -> Optional[str]:
        return None if self.is_folder else self._get_dropbox_hash()

    @abstractmethod
    def _get_size(self) -> int:
        pass

    def get_size(self) -> int:
        return 0 if self.is_folder else self._get_size()

    @abstractmethod
    def _get_modified_timestamp(self) -> float:
        pass

    def get_modified_timestamp(self) -> float:
        return 0. if self.is_folder else self._get_modified_timestamp()

    def __eq__(self, other: FileInfo) -> bool:
        if not isinstance(other, FileInfo):
            return False

        return (self.posix_path == other.posix_path and
                self.get_modified_timestamp() == other.get_modified_timestamp() and
                self.get_size() == other.get_size())

    def __hash__(self) -> int:
        return hash((self.posix_path, self.get_modified_timestamp(), self.get_size()))

    def __repr__(self):
        return f"FileInfo(path={self.relative_path}, is_folder={str(self.is_folder):s})"

    def __str__(self):
        return self.__repr__()


class LocalFile(FileInfo):
    def __init__(self, absolute_path: pathlib.PosixPath, local_folder: pathlib.PosixPath):
        relative_path = absolute_path.relative_to(local_folder)
        super().__init__(relative_path, absolute_path.is_dir())
        self.absolute_path = absolute_path
        self.dropbox_hash = None
        self.size = -1
        self.timestamp = -1.

    def _get_dropbox_hash(self) -> str:
        if self.dropbox_hash is None:
            self.dropbox_hash = compute_dropbox_hash(self.absolute_path)

        return self.dropbox_hash

    def _get_size(self) -> int:
        if self.size < 0:
            stat = self.absolute_path.stat()
            self.size = stat.st_size
        return self.size

    def _get_modified_timestamp(self) -> float:
        if self.timestamp < 0.:
            self.timestamp = get_mod_time_locally(self.absolute_path)

        return self.timestamp


class RemoteFile(FileInfo):
    def _get_dropbox_hash(self) -> str:
        return self.entry.content_hash

    def _get_size(self) -> int:
        return self.entry.size

    def _get_modified_timestamp(self) -> float:
        return self.entry.client_modified.timestamp()

    def __init__(self, entry: Union[files.FileMetadata, files.FolderMetadata], dropbox_folder: pathlib.PurePosixPath):
        absolute_path = pathlib.PurePosixPath(entry.path_display)
        relative_path = absolute_path.relative_to(dropbox_folder)
        super().__init__(relative_path, isinstance(entry, files.FolderMetadata))
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
