from __future__ import annotations
import json
import os
import time
from typing import Union

import dropbox
from dropbox import files as db_files
from dropbox.files import DeleteArg

import logging

from new_utils import PathInfo, compute_dropbox_hash, get_modification_timestamp_locally, \
    get_modification_timestamp_remotely, depth, FILE_LIST


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

        self.last_sync = os.path.join(self.info_folder, "last_sync.json")

    def get_local_files(self: DropboxSync) -> set[PathInfo]:
        """Get all files in the local folder recursively"""
        contents = set()
        length = len(self.local_folder)
        for root, dirs, file_paths in os.walk(self.local_folder):
            for each_path in file_paths:
                full_path = os.path.join(root, each_path)
                mod_dt = get_modification_timestamp_locally(full_path)
                db_hash = compute_dropbox_hash(full_path)
                each_file = PathInfo(full_path[length:], mod_dt, db_hash)
                contents.add(each_file)

            for each_path in dirs:
                full_path = os.path.join(root, each_path)
                mod_dt = get_modification_timestamp_locally(full_path)
                each_dir = PathInfo(f"{full_path[length:]:s}/", mod_dt, None)
                contents.add(each_dir)

        return contents

    def get_remote_files(self: DropboxSync) -> set[PathInfo]:
        """Get all files in the dropbox folder recursively"""
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
            if each_path.is_folder:
                self.client.files_create_folder_v2(dst_path)
                continue

            src_path = self.local_folder + each_path.path
            with open(src_path, mode="rb") as file:
                self.client.files_upload(file.read(), dst_path, mode=db_files.WriteMode('overwrite'))

    def download(self: DropboxSync, paths: set[PathInfo]) -> None:
        directories = [each_path for each_path in paths if each_path.is_folder]
        directories.sort(key=depth)

        for each_path in directories:
            dst_path = self.local_folder + each_path.path
            os.mkdir(dst_path)

        for each_path in paths:
            if not each_path.is_folder:
                src_path = self.dropbox_folder + each_path.path
                dst_path = self.local_folder + each_path.path
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

    @staticmethod
    def _get_file_list(file_paths: set[PathInfo]) -> FILE_LIST:
        return [
            {"path": each_path.path, "hash": each_path.hash, "timestamp": each_path.timestamp}
            for each_path in file_paths]

    def log_sync(self: DropboxSync,
                 local_files: set[PathInfo], remote_paths: set[PathInfo],
                 download_paths: set[PathInfo], upload_paths: set[PathInfo]) -> None:
        info = {"local":        DropboxSync._get_file_list(local_files),
                "remote":       DropboxSync._get_file_list(remote_paths),
                "downloads":    DropboxSync._get_file_list(download_paths),
                "uploads":      DropboxSync._get_file_list(upload_paths)}

        with open(self.last_sync, mode="w") as file:
            json.dump(info, file)

    def get_last_sync(self: DropboxSync) -> tuple[FILE_LIST, FILE_LIST, FILE_LIST, FILE_LIST]:
        with open(self.last_sync, mode="r") as file:
            info = json.load(file)

        return info["local"], info["remote"], info["downloads"], info["uploads"]

    def initial_sync(self: DropboxSync) -> None:
        local_files = self.get_local_files()
        remote_files = self.get_remote_files()

        upload_files = local_files - remote_files
        download_files = remote_files - local_files

        print(f"Uploading {len(upload_files):d} files")
        self.upload(upload_files)
        print(f"Downloading {len(download_files):d} files")
        self.download(download_files)

        self.log_sync(local_files, remote_files, download_files, upload_files)

    def sync(self: DropboxSync) -> None:
        local_files = self.get_local_files()
        remote_files = self.get_remote_files()

        # todo: dont store last uploads or downloads?
        last_locally, last_remotely, _, _ = self.get_last_sync()

        upload_files = set()
        download_files = set()

        delete_remotely = set()
        delete_locally = set()

        # consider last sync
        for each_path in remote_files | local_files:
            if each_path in remote_files and each_path in local_files:
                # check if the file has been modified? not necessary because hash and timestamp?
                # todo: always prefer newer
                continue

            here_not_there = each_path in local_files and each_path not in remote_files
            there_not_here = each_path in remote_files and each_path not in local_files

            from_there = each_path in last_locally
            from_here = each_path in last_remotely

            if here_not_there:
                if from_there:
                    delete_locally.add(each_path)
                else:
                    upload_files.add(each_path)

            if there_not_here:
                if from_here:
                    delete_remotely.add(each_path)
                else:
                    download_files.add(each_path)

        # upload_files = local_files - remote_files
        # download_files = remote_files - local_files

        print(f"Uploading {len(upload_files):d} files")
        self.upload(upload_files)
        print(f"Downloading {len(download_files):d} files")
        self.download(download_files)

        print(f"Deleting {len(delete_remotely):d} remote files")
        self.delete_remote_files(delete_remotely)
        print(f"Deleting {len(delete_locally):d} local files")
        self.delete_local_files(delete_locally)


def main() -> None:
    db_sync = DropboxSync("resources/config.json")
    db_sync.initial_sync()
    while True:
        db_sync.sync()
        time.sleep(5)


if __name__ == "__main__":
    main()
