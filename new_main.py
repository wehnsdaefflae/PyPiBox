from __future__ import annotations
import json
import os
import shutil
import time

import dropbox
from dropbox import files as db_files
from dropbox.files import DeleteArg


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

    def get_local_files(self: DropboxSync) -> set[str]:
        """Get all files in the local folder recursively"""
        contents = set()
        length = len(self.local_folder)
        for root, dirs, files in os.walk(self.local_folder):
            for each_file in files:
                contents.add(os.path.join(root, each_file)[length:])
            for each_dir in dirs:
                contents.add(f"{os.path.join(root, each_dir)[length:]:s}/")

        return contents

    def get_remote_files(self: DropboxSync) -> set[str]:
        """Get all files in the dropbox folder recursively"""
        contents = set()
        length = len(self.dropbox_folder)
        result = self.client.files_list_folder(self.dropbox_folder, recursive=True)

        while True:
            for entry in result.entries:
                if isinstance(entry, db_files.FileMetadata):
                    contents.add(entry.path_display[length:])
                elif isinstance(entry, db_files.FolderMetadata):
                    contents.add(f"{entry.path_display[length:]:s}/")

            if not result.has_more:
                break

            result = self.client.files_list_folder_continue(result.cursor)

        return contents

    def upload(self: DropboxSync, files: set[str]) -> None:
        for each_file in files:
            dst_path = self.dropbox_folder + each_file
            if each_file[-1] == "/":
                self.client.files_create_folder_v2(dst_path)
                continue

            src_path = self.local_folder + each_file
            with open(src_path, mode="rb") as file:
                self.client.files_upload(file.read(), dst_path, mode=db_files.WriteMode('overwrite'))

    def download(self: DropboxSync, files: set[str]) -> None:
        for each_file in files:
            if each_file[-1] == "/":
                dst_path = self.local_folder + each_file
                os.makedirs(dst_path, exist_ok=True)

        for each_file in files:
            if each_file[-1] != "/":
                src_path = self.dropbox_folder + each_file
                dst_path = self.local_folder + each_file
                self.client.files_download_to_file(dst_path, src_path)

    def delete_remote_files(self: DropboxSync, files: set[str]) -> None:
        entries = [DeleteArg(self.dropbox_folder + each_file) for each_file in files]
        self.client.files_delete_batch(entries)

    def delete_local_files(self: DropboxSync, files: set[str]) -> None:
        for each_file in files:
            if each_file[-1] != "/":
                path = self.local_folder + each_file
                os.remove(path)

        for each_file in files:
            if each_file[-1] == "/":
                path = self.local_folder + each_file
                shutil.rmtree(path, ignore_errors=True)

    def sync(self: DropboxSync) -> None:
        local_files = self.get_local_files()
        remote_files = self.get_remote_files()

        upload_files = local_files - remote_files
        download_files = remote_files - local_files

        print(f"Uploading {len(upload_files):d} files")
        self.upload(upload_files)
        print(f"Downloading {len(download_files):d} files")
        self.download(download_files)

        local_files |= upload_files
        remote_files |= download_files

        # fuck me. i'm done

        print(f"Deleting {len(remote_files - local_files):d} remote files")
        self.delete_remote_files(local_files - download_files)
        print(f"Deleting {len(local_files - remote_files):d} local files")
        self.delete_local_files(remote_files - upload_files)


def main() -> None:
    db_sync = DropboxSync("resources/config.json")
    while True:
        db_sync.sync()
        time.sleep(5)


if __name__ == "__main__":
    main()
