from __future__ import annotations
import json
import os
import time
from typing import Optional

import dropbox
from dropbox import files as db_files, exceptions as db_exceptions
from dropbox.files import DeleteArg

import logging

from new_utils import PathInfo, compute_dropbox_hash, get_mod_time_locally, get_mod_time_remotely, depth


class DropboxSync:
    def __init__(self: DropboxSync, config_path: str, debug: bool = True) -> None:
        with open(config_path, mode="r") as file:
            config = json.load(file)

        self.local_folder = config["local_folder"]
        assert self.local_folder[-1] == "/"
        os.makedirs(self.local_folder, exist_ok=True)

        self.dropbox_folder = config["dropbox_folder"]
        assert self.dropbox_folder[-1] == "/"
        if len(self.dropbox_folder) < 2:
            self.dropbox_folder = ""

        self.client = dropbox.Dropbox(config.pop("access_token"))

        self.log_file = os.path.join("events.log")
        logging.basicConfig(filename=self.log_file, level=logging.INFO)

        self.upload_files, self.download_files = set(), set()
        self.delete_remotely, self.delete_locally = set(), set()

        self.last_local_index = set()
        self.last_remote_index = set()

    def get_local_index(self: DropboxSync) -> set[PathInfo]:
        print("Getting local index...")
        local_file_index = set()
        length = len(self.local_folder)
        for root, dirs, file_paths in os.walk(self.local_folder):
            for each_path in file_paths:
                full_path = os.path.join(root, each_path)
                mod_dt = get_mod_time_locally(full_path)
                db_hash = compute_dropbox_hash(full_path)
                each_file = PathInfo(full_path[length:], mod_dt, db_hash)
                local_file_index.add(each_file)

            for each_path in dirs:
                full_path = os.path.join(root, each_path)
                mod_dt = get_mod_time_locally(full_path)
                each_dir = PathInfo(f"{full_path[length:]:s}/", mod_dt, None)
                local_file_index.add(each_dir)

        return local_file_index

    def get_remote_index(self: DropboxSync) -> set[PathInfo]:
        print("Getting remote index...")
        contents = set()
        length = len(self.dropbox_folder)
        result = self.client.files_list_folder(self.dropbox_folder, recursive=True)

        while True:
            for entry in result.entries:
                if isinstance(entry, db_files.FileMetadata):
                    mod_dt = get_mod_time_remotely(entry)
                    db_hash = entry.content_hash
                    each_file = PathInfo(entry.path_display[length:], mod_dt, db_hash)
                    contents.add(each_file)

                elif isinstance(entry, db_files.FolderMetadata):
                    truncated = entry.path_display[length:]
                    if 0 < len(truncated):
                        each_dir = PathInfo(f"{truncated:s}/", -1., None)
                        contents.add(each_dir)

            if not result.has_more:
                break

            result = self.client.files_list_folder_continue(result.cursor)

        return contents

    def upload(self: DropboxSync, paths: set[PathInfo]) -> None:
        len_paths = len(paths)
        if len_paths < 1:
            return
        print(f"Uploading {len_paths:d} files")

        for each_path in paths:
            dst_path = self.dropbox_folder + each_path.path
            remote_path = self.get_remote_path(each_path.path)

            if each_path.is_folder:
                if remote_path is not None and remote_path.is_folder:
                    continue

                self.client.files_create_folder_v2(dst_path)
                continue

            if remote_path is not None and not remote_path.is_folder and not remote_path.is_deleted:
                if remote_path.hash == each_path.hash or remote_path.timestamp >= each_path.timestamp:
                    continue

            src_path = self.local_folder + each_path.path
            with open(src_path, mode="rb") as file:
                self.client.files_upload(file.read(), dst_path, mode=db_files.WriteMode("overwrite"))

    def download(self: DropboxSync, paths: set[PathInfo]) -> None:
        len_paths = len(paths)
        if len(paths) < 1:
            return
        print(f"Downloading {len_paths:d} files")

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
                local_time = get_mod_time_locally(dst_path)
                if local_hash == each_path.hash or local_time >= each_path.timestamp:
                    continue

            self.client.files_download_to_file(dst_path, src_path)

    def delete_remote_files(self: DropboxSync, paths: set[PathInfo]) -> None:
        len_paths = len(paths)
        if len_paths < 1:
            return
        print(f"Deleting {len_paths:d} remote files")

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
        len_paths = len(paths)
        if len_paths < 1:
            return
        print(f"Deleting {len_paths:d} local files")

        for each_path in paths:
            if not each_path.is_folder:
                path = self.local_folder + each_path.path
                os.remove(path)

        folders = [every_path for every_path in paths if every_path.is_folder]
        folders.sort(key=depth, reverse=True)
        for each_path in folders:
            path = self.local_folder + each_path
            os.rmdir(path)

    def get_remote_path(self, each_path: str) -> Optional[PathInfo]:
        try:
            entry = self.client.files_get_metadata(self.dropbox_folder + each_path)
            if isinstance(entry, db_files.FolderMetadata):
                return PathInfo(f"{each_path}/", -1., None)

            if isinstance(entry, db_files.FileMetadata):
                return PathInfo(each_path, get_mod_time_remotely(entry), entry.content_hash)

        except db_exceptions.ApiError as err:
            if err.error.is_path() and err.error.get_path().is_not_found():
                return None
            else:
                raise err

    def get_remote_delta(self: DropboxSync) -> tuple[set[PathInfo], set[PathInfo]]:
        created = set()
        removed = set()

        result = self.client.files_list_folder_get_latest_cursor(
            self.dropbox_folder, recursive=True, include_deleted=True)

        while True:
            result = self.client.files_list_folder_continue(result.cursor)

            for each_entry in result.entries:
                if isinstance(each_entry, db_files.FolderMetadata):
                    mod_dt = get_mod_time_remotely(each_entry)
                    each_dir = PathInfo(f"{each_entry.path_display}/", mod_dt, None)
                    created.add(each_dir)

                elif isinstance(each_entry, db_files.FileMetadata):
                    mod_dt = get_mod_time_remotely(each_entry)
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

        self.upload(upload_files)
        self.download(download_files)

        self.last_local_index = local_index
        self.last_remote_index = remote_index

    def sync(self: DropboxSync) -> None:
        local_index = self.get_local_index()
        remote_index = self.get_remote_index()

        locally_added = local_index - self.last_local_index - self.download_files
        locally_removed = self.last_local_index - local_index - self.delete_locally
        remotely_added = remote_index - self.last_remote_index - self.upload_files
        remotely_removed = self.last_remote_index - remote_index - self.delete_remotely

        self.delete_remotely.clear()
        self.upload_files.clear()
        self.delete_locally.clear()
        self.download_files.clear()

        for each_path in remote_index | local_index:
            if each_path in locally_removed and each_path in remote_index:
                self.delete_remotely.add(each_path)

            elif each_path in locally_added and each_path not in remote_index:
                self.upload_files.add(each_path)

            elif each_path in remotely_removed and each_path in local_index:
                self.delete_locally.add(each_path)

            elif each_path in remotely_added and each_path not in local_index:
                self.download_files.add(each_path)

        self.delete_remote_files(self.delete_remotely)
        self.upload(self.upload_files)

        self.delete_local_files(self.delete_locally)
        self.download(self.download_files)

        self.last_local_index = local_index
        self.last_remote_index = remote_index


def main() -> None:
    db_sync = DropboxSync("resources/config.json")
    db_sync.initial_sync()

    while True:
        time.sleep(5)
        db_sync.sync()


if __name__ == "__main__":
    main()
