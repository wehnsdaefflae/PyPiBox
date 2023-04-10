# coding=utf-8
from __future__ import annotations
import json
import os
import sys
import pathlib
import time
from typing import Any, Optional, Iterable

import dropbox
from dropbox import files as db_files
from dropbox import exceptions as db_exceptions
from dropbox.files import DeleteArg

import logging

from utils import FILE_INDEX, get_mod_time_locally, depth, LOCAL_FILE_INDEX, LocalFile, REMOTE_FILE_INDEX, RemoteFile, get_size_locally

from utils import SyncDirection, SyncAction


class DropboxSync:
    @staticmethod
    def _logging_handlers() -> set[logging.StreamHandler]:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        handler_stdout = logging.StreamHandler(sys.stdout)
        handler_stdout.setLevel(logging.DEBUG)
        handler_stdout.setFormatter(formatter)

        handler_file = logging.FileHandler("events.log")
        handler_file.setLevel(logging.DEBUG)
        handler_file.setFormatter(formatter)

        return {handler_stdout, handler_file}

    def __init__(self: DropboxSync, app_key: str, app_secret: str, refresh_token: str,
                 interval_seconds: int,
                 local_folder: str, dropbox_folder: str,
                 debug: bool = True) -> None:

        self.client = dropbox.Dropbox(app_key=app_key, app_secret=app_secret, oauth2_refresh_token=refresh_token)

        self.main_logger = logging.getLogger()
        self.main_logger.setLevel(logging.DEBUG)
        for each_handler in DropboxSync._logging_handlers():
            self.main_logger.addHandler(each_handler)

        self.client.check_and_refresh_access_token()
        self.interval_seconds = interval_seconds
        self.local_folder = pathlib.PosixPath(local_folder)
        self.local_folder.mkdir(parents=True, exist_ok=True)

        self.dropbox_folder = pathlib.PurePosixPath(dropbox_folder)

        self.local_index = dict()
        self.remote_index = dict()
        self.last_local_index = dict()
        self.last_remote_index = dict()

        self.debug = debug

    def close(self: DropboxSync) -> None:
        self.client.close()

    @staticmethod
    def get_config(config_path: str) -> dict[str, Any]:
        with open(config_path, mode="r") as file:
            config = json.load(file)
        refresh_token = config.get("refresh_token", "")
        if len(refresh_token) < 1:
            authorization_flow = dropbox.DropboxOAuth2FlowNoRedirect(
                config["app_key"],
                consumer_secret=config["app_secret"],
                token_access_type="offline")

            authorization_url = authorization_flow.start()

            print(f"1. Go to: {authorization_url:s}")
            print("2. Click \"Allow\" (you might have to log in first).")
            print("3. Copy the authorization code.")
            auth_code = input("Enter the authorization code here: ").strip()

            oauth_result = authorization_flow.finish(auth_code)

            config["refresh_token"] = oauth_result.refresh_token

            with open(config_path, mode="w") as file:
                json.dump(config, file, indent=2)

            print("Authentication complete. Refresh token saved to config file.")
        return config

    def _get_local_index(self: DropboxSync) -> LOCAL_FILE_INDEX:
        self.main_logger.info("Getting local index...")
        local_file_index = dict()

        start_time = time.time()

        for i, each_path in enumerate(self.local_folder.rglob("*")):
            if i % 100 == 0:
                self.main_logger.info(f"Scanned {i:d} local files in {time.time() - start_time:.2f} seconds.")

            relative_path = each_path.relative_to(self.local_folder)
            pure_relative_path = pathlib.PurePosixPath(relative_path)

            cached_file = self.last_local_index.get(pure_relative_path, None)
            if cached_file is not None and \
                    cached_file.get_modified_timestamp() == get_mod_time_locally(each_path) and \
                    cached_file.get_size() == get_size_locally(each_path):
                local_file_index[pure_relative_path] = cached_file
                continue

            each_file = LocalFile(each_path)
            local_file_index[pure_relative_path] = each_file

        return local_file_index

    def _get_remote_index(self: DropboxSync) -> REMOTE_FILE_INDEX:
        self.main_logger.info("Getting remote index...")
        remote_index = dict()

        time_start = time.time()
        dropbox_folder_str = DropboxSync._dropbox_path_format(self.dropbox_folder)
        result = self.client.files_list_folder(dropbox_folder_str, recursive=True)

        while True:
            for entry in result.entries:
                if isinstance(entry, db_files.FileMetadata) or isinstance(entry, db_files.FolderMetadata):
                    each_file = RemoteFile(entry)
                    relative_path = each_file.path.relative_to(self.dropbox_folder)
                    remote_index[relative_path] = each_file

            if not result.has_more:
                break

            self.main_logger.info(f"Scanned {len(remote_index):d} remote files in {time.time() - time_start:.2f} seconds.")

            result = self.client.files_list_folder_continue(result.cursor)

        return remote_index

    def _upload_file(self, file_info: LocalFile, target_path: pathlib.PurePosixPath) -> None:
        megabyte = 1024 * 1024
        actual_file = file_info.actual
        file_size = file_info.get_size()
        chunk_size = 8 * megabyte
        with actual_file.open(mode="rb") as file:
            if file_size < chunk_size:
                self.main_logger.info(f"Uploading {file_info:s}...")
                db_target = DropboxSync._dropbox_path_format(target_path)
                self.client.files_upload(file.read(), db_target, mode=db_files.WriteMode("overwrite"))
                # https://github.com/dropbox/dropbox-sdk-python/blob/master/example/updown.py

            else:
                chunk = file.read(chunk_size)
                upload_session_start_result = self.client.files_upload_session_start(chunk)
                session_id = upload_session_start_result.session_id

                while (byte_position := file.tell()) < file_size:
                    progress = f"{byte_position / megabyte:.1f} / {file_size / megabyte:.1f} MB"
                    self.main_logger.info(f"Uploading {file_info:s} {progress:s}...")

                    chunk = file.read(chunk_size)
                    cursor = db_files.UploadSessionCursor(session_id=session_id, offset=byte_position)
                    if byte_position + len(chunk) < file_size:
                        self.client.files_upload_session_append_v2(chunk, cursor)
                    else:
                        db_target = DropboxSync._dropbox_path_format(target_path)
                        commit = db_files.CommitInfo(path=db_target, mode=db_files.WriteMode("overwrite"))
                        self.client.files_upload_session_finish(chunk, cursor, commit)

    def _method_upload(self: DropboxSync, local_index: LOCAL_FILE_INDEX) -> None:
        len_paths = len(local_index)
        if len_paths < 1:
            return

        folders = [(each_path, each_file) for each_path, each_file in local_index.items() if each_file.is_folder]
        self._create_folders_remotely(folders)

        files = [(each_path, each_file) for each_path, each_file in local_index.items() if not each_file.is_folder]
        self._upload_files(files)

    def _create_folders_remotely(self, folders: list[tuple[pathlib.PurePosixPath, LocalFile]]) -> None:
        self.main_logger.info(f"Creating {len(folders):d} folders...")

        for each_path, each_file in folders:
            dst_path = self.dropbox_folder / each_path
            remote_file = self._get_remote_file(dst_path)

            if remote_file is not None and remote_file.is_folder:
                continue

            dst_db = DropboxSync._dropbox_path_format(dst_path)
            self.client.files_create_folder_v2(dst_db)

    def _upload_files(self, files: list[tuple[pathlib.PurePosixPath, LocalFile]]) -> None:
        self.main_logger.info(f"Uploading {len(files):d} files...")

        for each_path, expected in files:
            dst_path = self.dropbox_folder / each_path
            remote_file = self._get_remote_file(dst_path)

            if remote_file is not None:
                if remote_file == expected:
                    continue

                if remote_file.get_modified_timestamp() >= expected.get_modified_timestamp():
                    self.main_logger.warning(f"Skipping conflict: More recent remote file {each_path:s}.")
                    continue

            self._upload_file(expected, dst_path)

    def _method_download(self: DropboxSync, remote_index: REMOTE_FILE_INDEX) -> None:
        len_paths = len(remote_index)
        if len(remote_index) < 1:
            return
        self.main_logger.info(f"Downloading {len_paths:d} files")

        local_dir_paths = set(self.local_folder / each_path for each_path, each_file in remote_index.items() if each_file.is_folder)
        for each_dir_path in local_dir_paths:
            each_dir_path.mkdir(exist_ok=True, parents=True)

        files = [(each_path, each_file) for each_path, each_file in remote_index.items() if not each_file.is_folder]
        for each_path, expected in files:
            local_path = self.local_folder / each_path
            local_file = LocalFile(local_path)
            if local_file is not None:
                continue
            elif local_file.is_folder:
                continue
            elif local_file.get_dropbox_hash() == expected.get_dropbox_hash():
                continue
            elif local_file.get_modified_timestamp() >= expected.get_modified_timestamp():
                self.main_logger.warning(f"Skipping conflict: More recent local file {each_path:s}.")
                continue

            db_remote_path = DropboxSync._dropbox_path_format(expected.path)
            db_local_path = DropboxSync._dropbox_path_format(local_path)
            self.client.files_download_to_file(db_local_path, db_remote_path)

            os.utime(db_local_path, (time.time(), expected.get_modified_timestamp()))

    def _get_remote_file(self, remote_path: pathlib.PurePath) -> Optional[RemoteFile]:
        db_path = DropboxSync._dropbox_path_format(remote_path)
        try:
            entry = self.client.files_get_metadata(db_path)

        except db_exceptions.ApiError as e:
            self.main_logger.warning(f"Could not get metadata for {remote_path:s}: {str(e):s}")
            return None

        if isinstance(entry, db_files.FileMetadata) or isinstance(entry, db_files.FolderMetadata):
            return RemoteFile(entry)

        return None

    def _method_delete_remote(self: DropboxSync, local_index: LOCAL_FILE_INDEX) -> None:
        len_paths = len(local_index)
        if len_paths < 1:
            return
        self.main_logger.warning(f"Deleting {len_paths:d} remote files")

        files = [(each_path, each_file) for each_path, each_file in local_index.items() if not each_file.is_folder]
        file_entries = self._get_files_to_delete_remotely(files)
        self._delete_batch(file_entries)

        folders = [(each_path, each_folder) for each_path, each_folder in local_index.items() if each_folder.is_folder]
        delete_args = self._get_folders_to_delete_remotely(folders)
        self._delete_batch(delete_args)

    def _get_folders_to_delete_remotely(self, folders: list[tuple[pathlib.PurePath, LocalFile]]) -> list[DeleteArg]:
        folders.sort(key=lambda x: depth(x[0]))
        deleted = set()
        delete_args = []
        for each_path, expected in folders:
            remote_path = self.dropbox_folder / each_path
            remote_file = self._get_remote_file(remote_path)
            if remote_file is None:
                continue

            elif not remote_file.is_folder:
                continue

            elif expected.get_modified_timestamp() < remote_file.get_modified_timestamp():
                self.main_logger.warning(f"Skipping conflict: Updated remote deletion target folder {each_path:s}.")
                continue

            each_posix = each_path.as_posix()
            if any(each_posix.startswith(each_deleted) for each_deleted in deleted):
                continue

            dropbox_path = DropboxSync._dropbox_path_format(remote_path)
            delete_arg = DeleteArg(dropbox_path)
            delete_args.append(delete_arg)
            deleted.add(each_posix)

        return delete_args

    def _get_files_to_delete_remotely(self, files: Iterable[tuple[pathlib.PurePath, LocalFile]]) -> list[DeleteArg]:
        file_entries = []
        for each_path, expected in files:
            if expected.is_folder:
                continue

            remote_path = self.dropbox_folder / each_path
            remote_file = self._get_remote_file(remote_path)
            if remote_file is None:
                continue

            elif remote_file.is_folder:
                continue

            elif (remote_file.get_dropbox_hash() != expected.get_dropbox_hash() or
                  expected.get_modified_timestamp() < remote_file.get_modified_timestamp()):
                self.main_logger.warning(f"Skipping conflict: Unexpected remote deletion target file {each_path:s}.")
                continue

            dropbox_path = DropboxSync._dropbox_path_format(remote_path)
            delete_arg = DeleteArg(dropbox_path)
            file_entries.append(delete_arg)

        return file_entries

    def _delete_batch(self, file_entries: list[DeleteArg]) -> None:
        len_files = len(file_entries)
        ids = set()
        for i in range(0, len_files, 1000):
            sub_list = file_entries[i:i + 1000]
            self.main_logger.warning(f"Deleting {len(sub_list):d} remote files...")
            async_job_launch: dropbox.files.DeleteBatchLaunch = self.client.files_delete_batch(sub_list)
            async_job_id = async_job_launch.get_async_job_id()
            ids.add(async_job_id)

        while 0 < len([(status := self.client.files_delete_batch_check(each_id)) for each_id in ids if not status.is_complete()]):
            self.main_logger.warning("Waiting for deletion to finish...")
            time.sleep(1)

    def _method_delete_local(self: DropboxSync, remote_index: REMOTE_FILE_INDEX) -> None:
        len_paths = len(remote_index)
        if len_paths < 1:
            return
        self.main_logger.warning(f"Deleting {len_paths:d} local files")

        for each_path, expected in remote_index.items():
            if not expected.is_folder:
                local_file = self.local_folder / each_path
                status = local_file.stat()
                if status.st_size != expected.get_size() or expected.get_modified_timestamp() < get_mod_time_locally(local_file):
                    self.main_logger.warning(f"Skipping conflict: Unexpected local deletion target file {each_path:s}.")
                    continue
                local_file.unlink()

        folders = [(each_path, each_file) for each_path, each_file in remote_index.items() if each_file.is_folder]
        folders.sort(key=lambda x: depth(x[0]), reverse=True)
        for each_path, expected in folders:
            each_local_folder = self.local_folder / each_path
            if expected.get_modified_timestamp() < get_mod_time_locally(each_local_folder):
                self.main_logger.warning(f"Skipping conflict: Unexpected local deletion target folder {each_path:s}.")
                continue
            each_local_folder.rmdir()

    def _sync_action(self, source_changes: FILE_INDEX, method: SyncAction, direction: SyncDirection, debug: bool):
        if direction == SyncDirection.UP:
            index_dst = self.remote_index

            if method == SyncAction.DEL:
                action = self._method_delete_remote
            elif method == SyncAction.ADD:
                action = self._method_upload
            else:
                raise Exception("Invalid method")

        elif direction == SyncDirection.DOWN:
            index_dst = self.local_index

            if method == SyncAction.DEL:
                action = self._method_delete_local
            elif method == SyncAction.ADD:
                action = self._method_download
            else:
                raise Exception("Invalid method")

        else:
            raise Exception("Invalid direction")

        action_cache = dict()
        for each_path, src_file in source_changes.items():
            dst_file = index_dst.get(each_path)
            if dst_file is None:
                if method == SyncAction.ADD:
                    action_cache[each_path] = src_file
                    index_dst[each_path] = src_file

                elif method == SyncAction.DEL:
                    self.main_logger.warning(f"Conflict {method:s} {each_path:s} {direction:s}: file to delete does not exist.")
                    continue

            elif method == SyncAction.ADD:
                if dst_file == src_file:
                    self.main_logger.warning(f"Conflict {method:s} {each_path:s} {direction:s}: same file already exists.")
                    continue

                elif dst_file.get_modified_timestamp() < src_file.get_modified_timestamp():
                    action_cache[each_path] = src_file
                    index_dst[each_path] = src_file

                else:
                    self.main_logger.warning(f"Conflict {method:s} {each_path:s} {direction:s}: source is older than target.")
                    continue

            elif method == SyncAction.DEL:
                if dst_file == src_file:
                    action_cache[each_path] = src_file
                    index_dst.pop(each_path, None)

                else:
                    self.main_logger.warning(f"Conflict {method:s} {each_path:s} {direction:s}: unexpected target file.")
                    continue

        if debug:
            self.main_logger.debug(f"Skipping action {action} on cache: {action_cache.keys()}")
        else:
            action(action_cache)

    @staticmethod
    def _dropbox_path_format(path: pathlib.PurePath) -> str:
        posix = path.as_posix()
        if posix == "/":
            return ""
        return posix

    def sync(self: DropboxSync) -> None:
        self.local_index = self._get_local_index()
        self.remote_index = self._get_remote_index()

        locally_modified = {each_path: each_file for each_path, each_file in self.local_index.items() if each_file != self.last_local_index.get(each_path)}
        remotely_modified = {each_path: each_file for each_path, each_file in self.remote_index.items() if each_file != self.last_remote_index.get(each_path)}
        locally_removed = {each_path: each_file for each_path, each_file in self.last_local_index.items() if each_path not in self.local_index}
        remotely_removed = {each_path: each_file for each_path, each_file in self.last_remote_index.items() if each_path not in self.remote_index}

        self._sync_action(locally_modified, SyncAction.ADD, SyncDirection.UP, self.debug)
        self._sync_action(locally_removed, SyncAction.DEL, SyncDirection.UP, self.debug)
        self._sync_action(remotely_modified, SyncAction.ADD, SyncDirection.DOWN, False)
        self._sync_action(remotely_removed, SyncAction.DEL, SyncDirection.DOWN, False)

        self.last_local_index = dict(self.local_index)
        self.last_remote_index = dict(self.remote_index)


def main() -> None:
    config_path = "config.json"
    config = DropboxSync.get_config(config_path)

    db_sync = DropboxSync(**config)

    while True:
        db_sync.sync()
        time.sleep(db_sync.interval_seconds)


if __name__ == "__main__":
    main()
