from __future__ import annotations
import json
import os
import time

import dropbox
from dropbox import files as db_files
from dropbox.files import DeleteArg

import logging

from new_utils import PathInfo, compute_dropbox_hash, get_modification_timestamp_locally, \
    get_modification_timestamp_remotely, depth


CURSOR = str


class DropboxSync:
    def __init__(self: DropboxSync, config_path: str) -> None:
        with open(config_path, mode="r") as file:
            config = json.load(file)

        self.local_folder = config["local_folder"]
        assert self.local_folder[-1] == "/"
        os.makedirs(self.local_folder, exist_ok=True)

        self.dropbox_folder = config["dropbox_folder"]
        assert self.dropbox_folder[-1] == "/"

        self.client = dropbox.Dropbox(config.pop("access_token"))

        self.info_folder = os.path.join(self.local_folder, ".local_pypibox/")
        os.makedirs(self.info_folder, exist_ok=True)

        self.log_file = os.path.join(self.local_folder, "events.log")
        logging.basicConfig(filename=self.log_file, level=logging.INFO)

        self.last_local_index = set()

    def get_local_index(self: DropboxSync) -> set[PathInfo]:
        local_file_index = set()
        length = len(self.local_folder)
        for root, dirs, file_paths in os.walk(self.local_folder):
            for each_path in file_paths:
                full_path = os.path.join(root, each_path)
                mod_dt = get_modification_timestamp_locally(full_path)
                db_hash = compute_dropbox_hash(full_path)
                each_file = PathInfo(full_path[length:], mod_dt, db_hash)
                local_file_index.add(each_file)

            for each_path in dirs:
                full_path = os.path.join(root, each_path)
                mod_dt = get_modification_timestamp_locally(full_path)
                each_dir = PathInfo(f"{full_path[length:]:s}/", mod_dt, None)
                local_file_index.add(each_dir)

        return local_file_index

    def get_remote_index(self: DropboxSync) -> set[PathInfo]:
        contents = set()
        length = len(self.dropbox_folder)
        result = self.client.files_list_folder(self.dropbox_folder, recursive=True)

        while True:
            for entry in result.entries:
                if isinstance(entry, db_files.FileMetadata):
                    mod_dt = get_modification_timestamp_remotely(entry)
                    db_hash = entry.content_hash
                    each_file = PathInfo(entry.path_display[length:], mod_dt, db_hash)
                    contents.add(each_file)

                elif isinstance(entry, db_files.FolderMetadata):
                    mod_dt = get_modification_timestamp_remotely(entry)
                    each_dir = PathInfo(f"{entry.path_display[length:]:s}/", mod_dt, None)
                    contents.add(each_dir)

            if not result.has_more:
                break

            result = self.client.files_list_folder_continue(result.cursor)

        return contents

    def upload(self: DropboxSync, paths: set[PathInfo]) -> None:
        for each_path in paths:
            dst_path = self.dropbox_folder + each_path.path
            entry = self.client.files_get_metadata(dst_path)

            if each_path.is_folder:
                if isinstance(entry, db_files.FolderMetadata):
                    continue

                self.client.files_create_folder_v2(dst_path)
                continue

            if isinstance(entry, db_files.FileMetadata):
                if entry.content_hash == each_path.hash or entry.client_modified >= each_path.timestamp:
                    continue

            src_path = self.local_folder + each_path.path
            with open(src_path, mode="rb") as file:
                self.client.files_upload(file.read(), dst_path, mode=db_files.WriteMode("overwrite"))

    def download(self: DropboxSync, paths: set[PathInfo]) -> None:
        directories = [each_path for each_path in paths if each_path.is_folder]
        directories.sort(key=depth)

        for each_path in directories:
            dst_path = self.local_folder + each_path.path
            if os.path.exists(dst_path):
                continue

            os.mkdir(dst_path)

        files = [each_path for each_path in paths if not each_path.is_folder]
        for each_path in files:
            src_path = self.dropbox_folder + each_path.path
            dst_path = self.local_folder + each_path.path
            if os.path.exists(dst_path):
                local_hash = compute_dropbox_hash(dst_path)
                local_time = get_modification_timestamp_locally(dst_path)
                if local_hash == each_path.hash or local_time >= each_path.timestamp:
                    continue

            self.client.files_download_to_file(dst_path, src_path)

    def delete_remote_files(self: DropboxSync, paths: set[PathInfo]) -> None:
        file_entries = [
            DeleteArg(self.dropbox_folder + each_path.path)
            for each_path in paths
            if not each_path.is_folder
        ]
        self.client.files_delete_batch(file_entries)

        folders = [each_path for each_path in paths if each_path.is_folder]
        folders.sort(key=depth, reverse=True)
        folder_entries = [DeleteArg(self.dropbox_folder + each_path.path) for each_path in folders]
        self.client.files_delete_batch(folder_entries)

    def delete_local_files(self: DropboxSync, paths: set[PathInfo]) -> None:
        for each_path in paths:
            if not each_path.is_folder:
                path = self.local_folder + each_path.path
                os.remove(path)

        folders = [every_path for every_path in paths if every_path.is_folder]
        folders.sort(key=depth, reverse=True)
        for each_path in folders:
            path = self.local_folder + each_path
            os.rmdir(path)

    def get_remote_delta(self: DropboxSync) -> tuple[set[PathInfo], set[PathInfo]]:
        created = set()
        removed = set()

        result = self.client.files_list_folder_get_latest_cursor(
            self.dropbox_folder, recursive=True, include_deleted=True, limit=2000)

        while True:
            result = self.client.files_list_folder_continue(result.cursor)
            for each_entry in result.entries:
                if isinstance(each_entry, db_files.FolderMetadata):
                    mod_dt = get_modification_timestamp_remotely(each_entry)
                    each_dir = PathInfo(f"{each_entry.path_display}/", mod_dt, None)
                    created.add(each_dir)

                elif isinstance(each_entry, db_files.FileMetadata):
                    mod_dt = get_modification_timestamp_remotely(each_entry)
                    db_hash = each_entry.content_hash
                    each_file = PathInfo(each_entry.path_display, mod_dt, db_hash)
                    created.add(each_file)

                elif isinstance(each_entry, db_files.DeletedMetadata):
                    each_path = PathInfo(each_entry.path_display, -1., None)
                    removed.add(each_path)

            if not result.has_more:
                break

        return created, removed

    def initial_sync(self: DropboxSync) -> None:
        local_index = self.get_local_index()
        remote_index = self.get_remote_index()

        upload_files = local_index - remote_index
        download_files = remote_index - local_index

        print(f"Uploading {len(upload_files):d} files")
        self.upload(upload_files)
        print(f"Downloading {len(download_files):d} files")
        self.download(download_files)

        self.last_local_index = local_index

    def sync(self: DropboxSync) -> None:
        local_index = self.get_local_index()
        remote_index = self.get_remote_index()

        locally_added, locally_removed = local_index - self.last_local_index, self.last_local_index - local_index
        remotely_added, remotely_removed = self.get_remote_delta()

        upload_files, download_files = set(), set()
        delete_remotely, delete_locally = set(), set()

        for each_path in remote_index | local_index:
            if each_path in locally_added and each_path not in remote_index:
                upload_files.add(each_path)

            elif each_path in locally_removed and each_path in remote_index:
                delete_remotely.add(each_path)

            elif each_path in remotely_added and each_path not in local_index:
                download_files.add(each_path)

            elif each_path in remotely_removed and each_path in local_index:
                delete_locally.add(each_path)

        print(f"Uploading {len(upload_files):d} files")
        self.upload(upload_files)
        print(f"Downloading {len(download_files):d} files")
        self.download(download_files)

        print(f"Deleting {len(delete_remotely):d} remote files")
        self.delete_remote_files(delete_remotely)
        print(f"Deleting {len(delete_locally):d} local files")
        self.delete_local_files(delete_locally)

        self.last_local_index = local_index


def main() -> None:
    db_sync = DropboxSync("resources/config.json")
    db_sync.initial_sync()
    while True:
        db_sync.sync()
        time.sleep(5)


if __name__ == "__main__":
    main()
