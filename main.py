import json
import time

import dropbox
from dropbox import files as db_files
from dropbox import exceptions as db_exceptions
from dropbox.files import RelocationPath
from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent, FileSystemMovedEvent


class DirectoryWatcher(FileSystemEventHandler):
    def __init__(self, upload: set[Path], delete: set[Path], move: set[tuple[Path, Path]], *args, **kwargs):
        self.upload = upload
        self.delete = delete
        self.move = move
        self.ignore = set()
        super().__init__(*args, **kwargs)

    def on_modified(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        file_path = event.src_path
        if file_path in self.ignore:
            return
        print(f"File locally modified: {file_path:s}")
        self.upload.add(Path(file_path))
        self.ignore.clear()

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        file_path = event.src_path
        if file_path in self.ignore:
            return
        print(f"File locally created: {event.src_path:s}")
        self.upload.add(Path(file_path))
        self.ignore.clear()

    def on_deleted(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        file_path = event.src_path
        if file_path in self.ignore:
            return
        print(f"File locally deleted: {file_path:s}")
        self.delete.add(Path(file_path))
        self.ignore.clear()

    def on_moved(self, event: FileSystemMovedEvent) -> None:
        src_path = event.src_path
        if src_path in self.ignore:
            return

        dest_path = event.dest_path
        if dest_path in self.ignore:
            return

        print(f"File locally moved from {src_path:s} to {dest_path:s}")
        self.move.add((event.src_path, event.dest_path))
        self.ignore.clear()


def get_all_local_entries(folder_path: Path) -> list[Path]:
    return list(folder_path.glob("**/*"))


CURSOR = str


def get_all_remote_entries(client: dropbox.Dropbox, path: Path) -> tuple[list[db_files.Metadata], CURSOR]:
    print(f"Getting all entries for {path.as_posix():s}...")
    dropbox_path = path.as_posix()
    if dropbox_path == "/":
        dropbox_path = ""

    result = client.files_list_folder(dropbox_path, recursive=True)
    entries = list(result.entries)

    while result.has_more:
        print(f"Found {len(entries):d} entries...")
        result = client.files_list_folder_continue(result.cursor)
        entries.extend(result.entries)

    return entries, result.cursor


def get_updated_remote_entries(client: dropbox.Dropbox, cursor: CURSOR) -> tuple[list[db_files.Metadata], CURSOR]:
    print("Getting updated entries...")
    result = client.files_list_folder_continue(cursor)
    entries = [each_entry for each_entry in result.entries if isinstance(each_entry, db_files.FileMetadata)]

    while result.has_more:
        print(f"Found {len(entries):d} updated entries...")
        result = client.files_list_folder_continue(result.cursor)
        for each_entry in result.entries:
            if isinstance(each_entry, db_files.FileMetadata):
                entries.append(each_entry)

    return entries, result.cursor


def apply_changes_locally(client: dropbox.Dropbox, dw: DirectoryWatcher, entries: list[db_files.Metadata], local_folder: Path) -> None:
    if len(entries) < 1:
        return

    for each_entry in entries:
        if isinstance(each_entry, db_files.FileMetadata):
            entry_path = Path(each_entry.path_display)
            local_path = local_folder / entry_path.relative_to("/")

            remote_mtime = each_entry.server_modified.timestamp()

            if local_path.exists():
                lstat = local_path.lstat()
                local_mtime = lstat.st_mtime

                if local_mtime > remote_mtime:
                    continue  # Local file is more recent, skip download, will be uploaded later

            elif not local_path.parent.exists():
                print(f"Creating folder {local_path.parent.as_posix():s}")
                local_path.parent.mkdir(parents=True)

            print(f"Downloading {each_entry.path_display:s}")
            file_path = local_path.as_posix()
            dw.ignore.add(file_path)
            client.files_download_to_file(local_path.as_posix(), each_entry.path_display)


def change_files_remotely(client: dropbox.Dropbox, local_files: set[Path], configuration: dict[str, str]) -> None:
    if len(local_files) < 1:
        return

    dropbox_folder = Path(configuration["dropbox_folder"])
    local_folder = Path(configuration["local_folder"])

    for local_path in local_files:
        relative_path = local_path.relative_to(local_folder)
        dropbox_path = dropbox_folder / relative_path

        try:
            remote_entry = client.files_get_metadata(dropbox_path.as_posix())
            if isinstance(remote_entry, db_files.FileMetadata):
                remote_mtime = remote_entry.server_modified.timestamp()
                lstat = local_path.lstat()
                local_mtime = lstat.st_mtime

                if remote_mtime >= local_mtime:
                    continue  # Remote file is more recent or same, skip upload

            with local_path.open(mode='rb') as f:
                print(f"Uploading {relative_path.as_posix():s}")
                client.files_upload(f.read(), dropbox_path.as_posix(), mode=db_files.WriteMode('overwrite'))

        except db_exceptions.ApiError as e:
            if e.error.is_path() and e.error.get_path().is_conflict():
                print(f"Conflict detected: {relative_path.as_posix():s}")
                continue

            elif not (e.error.is_path() and e.error.get_path().is_not_found()):
                raise

    local_files.clear()


def delete_files_remotely(client: dropbox.Dropbox, deleted_files: set[Path], configuration: dict[str, str]) -> None:
    if len(deleted_files) < 1:
        return

    dropbox_folder = Path(configuration["dropbox_folder"])
    local_folder = Path(configuration["local_folder"])

    print(f"Deleting {len(deleted_files):d} files")
    remote_files = [dropbox_folder / each_file.relative_to(local_folder) for each_file in deleted_files]
    client.files_delete_batch(remote_files)

    deleted_files.clear()


def move_files_remotely(client: dropbox.Dropbox, moved_files: set[RelocationPath], configuration: dict[str, str]) -> None:
    if len(moved_files) < 1:
        return

    dropbox_folder = Path(configuration["dropbox_folder"])
    local_folder = Path(configuration["local_folder"])

    print(f"Moving {len(moved_files):d} files")
    relocation_paths = [
        RelocationPath(
            from_path=dropbox_folder / src_path.relative_to(local_folder),
            to_path=dropbox_folder / dst_path.relative_to(local_folder)
        )
        for src_path, dst_path in moved_files]
    client.files_move_batch_v2(relocation_paths)

    moved_files.clear()


if __name__ == '__main__':
    with open("resources/config.json", mode="r") as file:
        config = json.load(file)

    dropbox_client = dropbox.Dropbox(config.pop("access_token"))

    locally_modified = set()
    locally_deleted = set()
    locally_moved = set()

    print("Starting Dropbox sync client...")
    Path(config["local_folder"]).mkdir(parents=True, exist_ok=False)

    event_handler = DirectoryWatcher(locally_modified, locally_deleted, locally_moved)
    observer = Observer()
    observer.schedule(event_handler, config["local_folder"], recursive=True)

    remote_entries, main_cursor = get_all_remote_entries(dropbox_client, Path(config["dropbox_folder"]))
    apply_changes_locally(dropbox_client, event_handler, remote_entries, Path(config["local_folder"]))

    # todo: determine initial delta

    observer.start()
    try:
        while True:
            delete_files_remotely(dropbox_client, locally_deleted, config)
            move_files_remotely(dropbox_client, locally_moved, config)
            change_files_remotely(dropbox_client, locally_modified, config)

            time.sleep(10)

            remote_entries, main_cursor = get_updated_remote_entries(dropbox_client, main_cursor)
            print(f"Updated remote {len(remote_entries):d} entries:")
            for _entry in remote_entries:
                print(f"  {_entry.path_display:s}")

            apply_changes_locally(dropbox_client, event_handler, remote_entries, Path(config["local_folder"]))

    except KeyboardInterrupt:
        observer.stop()
        print("Sync client stopped.")

    observer.join()
