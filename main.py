from __future__ import annotations

import json
import os

import time

import dropbox
from dropbox import files as db_files
from dropbox import exceptions as db_exceptions
from dropbox.files import RelocationPath, ListFolderResult, DeleteArg, ListRevisionsResult
from pathlib import Path

from watchdog.observers import Observer

from fsTools import DirectoryWatcher
from utils import local_ts_to_dt, utc_ts_to_dt, convert_remote_entry, path_local_to_remote, path_remote_to_local, \
    FileInfo
from windy_hasher import compute_dropbox_hash


def get_all_local_files(folder_path: Path) -> dict[Path, FileInfo]:
    entries = dict()
    for each_path in folder_path.glob("**/*"):
        if each_path.is_dir():
            continue
        stat = each_path.stat()
        local_datetime = local_ts_to_dt(stat.st_mtime)
        db_hash = compute_dropbox_hash(each_path)
        entries[each_path] = FileInfo(each_path,  local_datetime, db_hash)

    return entries


def get_remote_files(client: dropbox.Dropbox, result: ListFolderResult, only_deleted: bool = False) -> tuple[dict[Path, FileInfo], CURSOR]:
    remote_files = dict()
    remote_files_deleted = dict()

    while True:
        for each_entry in result.entries:
            is_file = isinstance(each_entry, db_files.FileMetadata)
            is_deleted = isinstance(each_entry, db_files.DeletedMetadata)
            if not is_file and not is_deleted:
                continue

            each_file = convert_remote_entry(each_entry)
            if is_file:
                remote_files[each_file.path] = each_file
            elif only_deleted and is_deleted:
                remote_files_deleted[each_file.path] = each_file

        if not result.has_more:
            break
        result = client.files_list_folder_continue(result.cursor)

    if only_deleted:
        for each_path in remote_files:
            remote_files_deleted.pop(each_path, None)

        return remote_files_deleted, result.cursor

    return remote_files, result.cursor


def get_all_remote_files(client: dropbox.Dropbox, dropbox_folder: Path, only_deleted: bool = False) -> tuple[dict[Path, FileInfo], CURSOR]:
    dropbox_path = dropbox_folder.as_posix()
    if dropbox_path == "/":
        dropbox_path = ""

    result = client.files_list_folder(dropbox_path, recursive=True, include_deleted=only_deleted)
    return get_remote_files(client, result, only_deleted=only_deleted)


def delta_transfers(src_files: dict[Path, FileInfo], dst_files: dict[Path, FileInfo]) -> dict[Path, FileInfo]:
    delta = dict()
    for each_path, each_file in src_files.items():
        dst_file = dst_files.get(each_path, None)
        if dst_file is None or (dst_file.hash != each_file.hash and dst_file.time < each_file.time):
            delta[each_path] = each_file

    return delta


CURSOR = str


def remote_changes(client: dropbox.Dropbox, cursor: CURSOR) -> tuple[dict[Path, FileInfo], CURSOR]:
    result = client.files_list_folder_continue(cursor)
    return get_remote_files(client, result)


def download_changes(client: dropbox.Dropbox, dw: DirectoryWatcher, remote_files: dict[Path, FileInfo], dropbox_folder: Path, local_folder: Path) -> None:
    if len(remote_files) < 1:
        return

    total = len(remote_files)
    remote_files_copy = remote_files.copy()
    for i, (each_path, each_file) in enumerate(remote_files_copy.items()):
        local_path = path_remote_to_local(each_path, local_folder, dropbox_folder)

        if local_path.exists():
            try:
                lstat = local_path.lstat()
                local_dt = local_ts_to_dt(lstat.st_mtime)
                if each_file.hash == compute_dropbox_hash(local_path) or each_file.time < local_dt:
                    continue  # Local file is more recent or identical, skip download, might be uploaded later

            except OSError as e:
                print(f"File {local_path.as_posix():s} is not accessible, skipping download")
                continue

        elif not local_path.parent.exists():
            print(f"Creating folder {local_path.parent.as_posix():s}")
            local_path.parent.mkdir(parents=True)

        print(f"Downloading {i+1:d}/{total:d} {each_path.as_posix():s}")

        file_path = local_path.as_posix()
        dw.ignore.add(file_path)
        client.files_download_to_file(local_path.as_posix(), each_path.as_posix())
        dw.ignore.remove(file_path)


def upload_changes(client: dropbox.Dropbox, local_files: dict[Path, FileInfo], dropbox_folder: Path, local_folder: Path) -> None:
    if len(local_files) < 1:
        return

    total = len(local_files)
    local_files_copy = local_files.copy()
    for i, (local_path, local_file) in enumerate(local_files_copy.items()):
        remote_path = path_local_to_remote(local_path, local_folder, dropbox_folder)

        try:
            remote_entry = client.files_get_metadata(remote_path.as_posix())
            if isinstance(remote_entry, db_files.FileMetadata):
                remote_ts = remote_entry.server_modified.timestamp()
                remote_dt = utc_ts_to_dt(remote_ts)

                if remote_entry.content_hash == local_file.hash or remote_dt >= local_file.time:
                    continue  # Remote file is identical, more recent, or same, skip upload

        except db_exceptions.ApiError as e:
            if not (e.error.is_path() and e.error.get_path().is_not_found()):
                # File exists, but error
                raise e

        try:
            with local_path.open(mode='rb') as f:
                print(f"Uploading {i + 1:d}/{total:d} {local_path.as_posix():s}")
                client.files_upload(f.read(), remote_path.as_posix(), mode=db_files.WriteMode('overwrite'))

        except FileNotFoundError:
            print(f"File {local_path.as_posix():s} not found, skipping")

    local_files.clear()


def process_remote_file_deletions(client: dropbox.Dropbox, local_files: set[FileInfo], local_folder: Path, dropbox_folder: Path) -> None:
    if len(local_files) < 1:
        return

    print(f"Deleting {len(local_files):d} files")
    remote_files = [
        path_local_to_remote(each_file.path, local_folder, dropbox_folder)
        for each_file in local_files
    ]

    client.files_delete_batch([DeleteArg(each_file.as_posix()) for each_file in remote_files])
    local_files.clear()


def move_files_remotely(client: dropbox.Dropbox, remote_relocation_paths: set[RelocationPath]) -> None:
    if len(remote_relocation_paths) < 1:
        return

    print(f"Moving {len(remote_relocation_paths):d} files")
    client.files_move_batch_v2(remote_relocation_paths)


def process_relocations(locally_moved_files: dict[tuple[Path, Path], FileInfo], local_folder: Path, dropbox_folder: Path) -> set[RelocationPath]:
    relocation_paths = {
        RelocationPath(
            from_path=path_local_to_remote(src_path, local_folder, dropbox_folder),
            to_path=path_local_to_remote(dst_path, local_folder, dropbox_folder)
        )
        for (src_path, dst_path), dst_file_info in locally_moved_files.items()}
    locally_moved_files.clear()
    return relocation_paths


def get_remotely_deleted_files(client: dropbox.Dropbox, local_folder: Path, dropbox_folder: Path) -> dict[Path, FileInfo]:
    complete_remote_files_absolute, _ = get_all_remote_files(client, dropbox_folder, only_deleted=True)

    to_delete = dict()
    for each_path, each_file in complete_remote_files_absolute.items():
        list_revisions_result: ListRevisionsResult = client.files_list_revisions(path=each_path.as_posix(), limit=1)
        last_revision, = list_revisions_result.entries
        if not isinstance(last_revision, db_files.FileMetadata):
            continue

        remote_dt = utc_ts_to_dt(last_revision.server_modified.timestamp())

        local_path = path_remote_to_local(each_path, local_folder, dropbox_folder)
        if not local_path.is_file():
            continue

        stat = local_path.stat()
        local_dt = local_ts_to_dt(stat.st_mtime)
        if each_file.is_deleted and local_path.is_file() and local_dt < remote_dt:
            to_delete[local_path] = each_file

    return to_delete


def delete_local_files(files: dict[Path, FileInfo], local_folder: Path) -> None:
    if len(files) < 1:
        return

    for each_path, _ in files.items():
        each_path.unlink(missing_ok=True)
        parent_path = each_path.parent
        if parent_path.is_dir() and parent_path != local_folder and len(os.listdir(parent_path.as_posix())) < 1:
            parent_path.rmdir()


def main() -> None:
    # todo: doesnt sync remote deletions

    with open("resources/config.json", mode="r") as file:
        config = json.load(file)

    dropbox_client = dropbox.Dropbox(config.pop("access_token"))

    print("Starting Dropbox sync client...")
    local_folder = Path(config["local_folder"])
    local_folder.mkdir(parents=True, exist_ok=True)

    dropbox_folder = Path(config["dropbox_folder"])
    try:
        dropbox_client.files_get_metadata(dropbox_folder.as_posix())
    except db_exceptions.ApiError as e:
        if e.error.is_path() and e.error.get_path().is_not_found():
            dropbox_client.files_create_folder_v2(dropbox_folder.as_posix())

    local_files_absolute = get_all_local_files(local_folder)
    print(f"Found {len(local_files_absolute):d} local files")
    local_files_relative = {
        each_path.relative_to(local_folder): each_time
        for each_path, each_time in local_files_absolute.items()}
    remote_files_absolute, main_cursor = get_all_remote_files(dropbox_client, dropbox_folder)
    print(f"Found {len(remote_files_absolute):d} remote files")
    remote_files_relative = {
        each_path.relative_to(dropbox_folder): each_time
        for each_path, each_time in remote_files_absolute.items()}

    locally_deleted_files = set()
    locally_moved_files = dict()
    locally_modified_files = dict()

    directory_watcher = DirectoryWatcher(locally_modified_files, locally_deleted_files, locally_moved_files)
    observer = Observer()
    observer.schedule(directory_watcher, local_folder.as_posix(), recursive=True)

    uploads_relative = delta_transfers(local_files_relative, remote_files_relative)
    uploads = {local_folder / each_path: each_time for each_path, each_time in uploads_relative.items()}
    upload_changes(dropbox_client, uploads, dropbox_folder, local_folder)

    downloads_relative = delta_transfers(remote_files_relative, local_files_relative)
    downloads = {dropbox_folder / each_path: each_time for each_path, each_time in downloads_relative.items()}
    download_changes(dropbox_client, directory_watcher, downloads, dropbox_folder, local_folder)

    print("Starting directory watcher...")
    observer.start()
    try:
        while True:
            print("Syncing remote...")
            # delete remotely
            process_remote_file_deletions(dropbox_client, locally_deleted_files, local_folder, dropbox_folder)

            # move remotely
            relocation_paths = process_relocations(locally_moved_files, local_folder, dropbox_folder)
            move_files_remotely(dropbox_client, relocation_paths)

            # upload remotely
            upload_changes(dropbox_client, locally_modified_files, dropbox_folder, local_folder)

            print("Sleeping...")
            time.sleep(5)

            print("Syncing local...")
            # download
            remote_files_absolute, main_cursor = remote_changes(dropbox_client, main_cursor)
            # print(f"Updated remote {len(remote_files_absolute):d} entries:")
            download_changes(dropbox_client, directory_watcher, remote_files_absolute, dropbox_folder, local_folder)

            # delete
            remotely_deleted = get_remotely_deleted_files(dropbox_client, local_folder, dropbox_folder)
            delete_local_files(remotely_deleted, local_folder)

            # todo: remotely deleted files not synced locally. remotely moved files?

    except KeyboardInterrupt:
        observer.stop()
        print("Sync client stopped.")

    observer.join()


if __name__ == '__main__':
    main()
