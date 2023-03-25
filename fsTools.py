from __future__ import annotations

import datetime
from pathlib import Path
from typing import Callable

from watchdog.events import FileSystemEventHandler, FileSystemEvent, FileSystemMovedEvent

from utils import get_mod_time, FileInfo
from windy_hasher import compute_dropbox_hash


class DirectoryWatcher(FileSystemEventHandler):
    class IgnoreDirectories:
        def __init__(self, method: Callable[[FileSystemEvent], None]):
            self.method = method

        def __get__(self, instance: DirectoryWatcher, owner) -> Callable[[FileSystemEvent], None]:
            def wrapper(event: FileSystemEvent) -> None:
                if event.is_directory:
                    return

                if event.src_path in instance.ignore:
                    return

                if isinstance(event, FileSystemMovedEvent):
                    if event.dest_path in instance.ignore:
                        return

                return self.method(instance, event)

            return wrapper

    def __init__(self, upload: dict[Path, FileInfo], delete: set[FileInfo], move: dict[tuple[Path, Path], FileInfo], *args, **kwargs):
        self.upload = upload
        self.delete = delete
        self.move = move
        self.ignore = set()
        super().__init__(*args, **kwargs)

    @IgnoreDirectories
    def on_modified(self, event: FileSystemEvent) -> None:
        file_name = event.src_path
        print(f"File locally modified: {file_name:s}")
        file_path = Path(file_name)
        dt = get_mod_time(file_path)
        db_hash = compute_dropbox_hash(file_path)
        self.upload[file_path] = FileInfo(file_path, dt, db_hash)

    @IgnoreDirectories
    def on_created(self, event: FileSystemEvent) -> None:
        file_name = event.src_path
        print(f"File locally created: {file_name:s}")
        file_path = Path(file_name)
        dt = get_mod_time(file_path)
        db_hash = compute_dropbox_hash(file_path)
        self.upload[file_path] = FileInfo(file_path, dt, db_hash)

    @IgnoreDirectories
    def on_deleted(self, event: FileSystemEvent) -> None:
        file_name = event.src_path
        print(f"File locally deleted: {file_name:s}")
        file_path = Path(file_name)
        dt = datetime.datetime.now()
        self.delete.add(FileInfo(file_path, dt, None, is_deleted=True))

    @IgnoreDirectories
    def on_moved(self, event: FileSystemMovedEvent) -> None:
        src_path = event.src_path
        dest_path = event.dest_path
        print(f"File locally moved from {src_path:s} to {dest_path:s}")
        src_path = Path(src_path)
        dest_path = Path(dest_path)
        dt = get_mod_time(dest_path)
        db_hash = compute_dropbox_hash(dest_path)
        self.move[(src_path, dest_path)] = FileInfo(dest_path, dt, db_hash)
