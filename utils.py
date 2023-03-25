from __future__ import annotations

import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import pytz
from dateutil import tz
from dropbox import files


def get_mod_time(file_path: Path) -> datetime.datetime:
    stat = file_path.stat()
    return local_ts_to_dt(stat.st_mtime)


def local_ts_to_dt(ts: float) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ts, tz=tz.tzlocal())


def utc_ts_to_dt(ts: float) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ts, tz=pytz.utc)


@dataclass
class FileInfo:
    path: Path
    time: datetime.datetime
    hash: Optional[str]
    is_deleted: bool = False

    def __eq__(self, other: FileInfo) -> bool:
        if not isinstance(other, FileInfo):
            return False
        return self.path == other.path and self.time == other.time and self.hash == other.hash

    def __hash__(self) -> int:
        return hash((self.path, self.hash))


def convert_remote_entry(entry: Union[files.FileMetadata, files.DeletedMetadata]) -> FileInfo:
    path = Path(entry.path_display)
    if isinstance(entry, files.DeletedMetadata):
        dt = datetime.datetime.now()
        return FileInfo(path, dt, None, is_deleted=True)

    ts = entry.server_modified.timestamp()
    dt = utc_ts_to_dt(ts)
    return FileInfo(path, dt, entry.content_hash)


def path_remote_to_local(path: Path, local_folder: Path, dropbox_folder: Path) -> Path:
    return local_folder / path.relative_to(dropbox_folder)


def path_local_to_remote(path: Path, local_folder: Path, dropbox_folder: Path) -> Path:
    return dropbox_folder / path.relative_to(local_folder)
