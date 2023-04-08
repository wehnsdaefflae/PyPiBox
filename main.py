# coding=utf-8
from __future__ import annotations
import json
import sys
import pathlib
import time
from typing import Optional, Any

import dropbox
from dropbox import files as db_files, exceptions as db_exceptions
from dropbox.files import DeleteArg

import logging

from utils import FileInfo, FILE_INDEX, compute_dropbox_hash, get_mod_time_locally, get_mod_time_remotely, depth, Delta

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

    def __init__(self: DropboxSync, app_key: str, app_secret: str, refresh_token: str, interval_seconds: int, local_folder: str, dropbox_folder: str) -> None:
        self.client = dropbox.Dropbox(app_key=app_key, app_secret=app_secret,oauth2_refresh_token=refresh_token)

        self.main_logger = logging.getLogger()
        self.main_logger.setLevel(logging.DEBUG)
        for each_handler in DropboxSync._logging_handlers():
            self.main_logger.addHandler(each_handler)

        self.client.check_and_refresh_access_token()
        self.interval_seconds = interval_seconds
        self.local_folder = pathlib.Path(local_folder)
        self.local_folder.mkdir(parents=True, exist_ok=True)

        self.dropbox_folder = pathlib.PurePosixPath(dropbox_folder)

        self.uploaded, self.downloaded = dict(), dict()
        self.deleted_remotely, self.deleted_locally = dict(), dict()

        self.last_local_index = dict()
        self.last_remote_index = dict()

        self.time_offset = -1.

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

    def _get_local_index(self: DropboxSync) -> FILE_INDEX:
        self.main_logger.info("Getting local index...")
        local_file_index = dict()

        for each_path in self.local_folder.rglob("*"):
            mod_dt = get_mod_time_locally(each_path)
            pure_path = pathlib.PurePosixPath(each_path)
            pure_relative_path = pure_path.relative_to(self.local_folder)

            if each_path.is_file():
                db_hash = compute_dropbox_hash(each_path)
                each_file = FileInfo(pure_path, mod_dt, False, dropbox_hash=db_hash)
                local_file_index[pure_relative_path] = each_file

            elif each_path.is_dir():
                each_dir = FileInfo(pure_path, mod_dt, True)
                local_file_index[pure_relative_path] = each_dir

        return local_file_index

    def _get_remote_index(self: DropboxSync) -> FILE_INDEX:
        self.main_logger.info("Getting remote index...")
        remote_index = dict()

        dropbox_folder_str = DropboxSync._dropbox_path_format(self.dropbox_folder)
        result = self.client.files_list_folder(dropbox_folder_str, recursive=True)

        while True:
            for entry in result.entries:
                if isinstance(entry, db_files.FileMetadata):
                    mod_dt = get_mod_time_remotely(entry, offset=self.time_offset)
                    db_hash = entry.content_hash
                    path = pathlib.PurePosixPath(entry.path_display)
                    each_file = FileInfo(path, mod_dt, db_hash)
                    remote_index[path.relative_to(self.dropbox_folder)] = each_file

                elif isinstance(entry, db_files.FolderMetadata):
                    path = pathlib.PurePosixPath(entry.path_display)
                    if path.as_posix() == self.dropbox_folder.as_posix():
                        continue
                    each_dir = FileInfo(path, -1., True, dropbox_hash=None)
                    remote_index[each_dir.path.relative_to(self.dropbox_folder)] = each_dir

            if not result.has_more:
                break

            result = self.client.files_list_folder_continue(result.cursor)

        return remote_index

    def _upload_file(self, file_info: FileInfo, target_path: pathlib.PurePosixPath) -> None:
        megabyte = 1024 * 1024
        actual_file = file_info.actual
        stat = actual_file.stat()
        file_size = stat.st_size
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

    def _method_upload(self: DropboxSync, file_index: FILE_INDEX) -> None:
        len_paths = len(file_index)
        if len_paths < 1:
            return
        self.main_logger.info(f"Uploading {len_paths:d} files")

        for each_path, each_file in file_index.items():
            dst_path = self.dropbox_folder / each_path
            remote_file = self._get_remote_file(self.dropbox_folder / each_path)

            if each_file.is_folder:
                if remote_file is not None and remote_file.is_folder:
                    continue

                dst_db = DropboxSync._dropbox_path_format(dst_path)
                self.client.files_create_folder_v2(dst_db)
                continue

            # necessary in case of intermediate upload from somewhere else!
            if remote_file is not None and not remote_file.is_folder and not remote_file.is_deleted:
                if remote_file == each_file:
                    continue

                if remote_file.timestamp >= each_file.timestamp:
                    self.main_logger.warning(f"Conflict: More recent remote file {each_path:s}. Skipping...")
                    continue

            self._upload_file(each_file, dst_path)

    def _method_download(self: DropboxSync, file_index: FILE_INDEX) -> None:
        len_paths = len(file_index)
        if len(file_index) < 1:
            return
        self.main_logger.info(f"Downloading {len_paths:d} files")

        for each_dir_path in set(self.local_folder / each_path for each_path, each_file in file_index.items() if each_file.is_folder):
            local_path = self.local_folder / each_dir_path
            local_path.mkdir(exist_ok=True, parents=True)

        for each_path, each_file in file_index.items():
            if each_file.is_folder:
                continue
            local_path = self.local_folder / each_path
            if local_path.is_file():
                local_hash = compute_dropbox_hash(local_path)
                if local_hash == each_file.dropbox_hash:
                    continue

                local_time = get_mod_time_locally(local_path)
                if local_time >= each_file.timestamp:
                    self.main_logger.warning(f"Conflict: More recent local file {each_path:s}. Skipping...")
                    continue

            remote_path = self.dropbox_folder / each_path
            db_remote_path = DropboxSync._dropbox_path_format(remote_path)
            db_local_path = DropboxSync._dropbox_path_format(local_path)
            self.client.files_download_to_file(db_local_path, db_remote_path)

    def _method_delete_remote(self: DropboxSync, file_index: FILE_INDEX) -> None:
        len_paths = len(file_index)
        if len_paths < 1:
            return
        self.main_logger.warning(f"Deleting {len_paths:d} remote files")

        file_entries = [
            DeleteArg(DropboxSync._dropbox_path_format(self.dropbox_folder / each_path))
            for each_path, each_file in file_index.items()
            if not each_file.is_folder]
        self.client.files_delete_batch(file_entries)

        folders = [each_path for each_path, each_file in file_index.items() if each_file.is_folder]
        folders.sort(key=depth)
        deleted = set()
        delete_args = []
        for each_path in folders:
            each_posix = each_path.as_posix()
            if any(each_posix.startswith(each_deleted) for each_deleted in deleted):
                continue
            dropbox_path = DropboxSync._dropbox_path_format(self.dropbox_folder / each_path)
            delete_args.append(DeleteArg(dropbox_path))
            deleted.add(each_posix)

        for i in range(0, len(delete_args), 1000):
            self.client.files_delete_batch(delete_args[i:i + 1000])

    def _method_delete_local(self: DropboxSync, file_index: FILE_INDEX) -> None:
        len_paths = len(file_index)
        if len_paths < 1:
            return
        self.main_logger.warning(f"Deleting {len_paths:d} local files")

        for each_path, each_file in file_index.items():
            if not each_file.is_folder:
                path = self.local_folder / each_path
                path.unlink()

        folders = [self.local_folder / each_path for each_path, each_file in file_index.items() if each_file.is_folder]
        folders.sort(key=depth, reverse=True)
        for each_path in folders:
            each_path.rmdir()

    def _get_remote_file(self, path: pathlib.PurePosixPath) -> Optional[FileInfo]:
        each_path = DropboxSync._dropbox_path_format(path)
        try:
            entry = self.client.files_get_metadata(each_path)
            if isinstance(entry, db_files.FolderMetadata):
                return FileInfo(path, -1., True, dropbox_hash=None)

            if isinstance(entry, db_files.FileMetadata):
                timestamp = get_mod_time_remotely(entry, offset=self.time_offset)
                return FileInfo(path, timestamp, False, dropbox_hash=entry.content_hash)

        except db_exceptions.ApiError as err:
            if err.error.is_path() and err.error.get_path().is_not_found():
                return None
            else:
                raise err

    def _get_deltas(self, local_index: FILE_INDEX, remote_index: FILE_INDEX) -> tuple[Delta, Delta]:
        locally_modified = {
            each_path: each_file
            for each_path, each_file in local_index.items()
            if (each_file != self.last_local_index.get(each_path) and each_path not in self.downloaded)}

        remotely_modified = {
            each_path: each_file
            for each_path, each_file in remote_index.items()
            if (each_file != self.last_remote_index.get(each_path) and each_path not in self.uploaded)}

        locally_removed = {
            each_path: each_file
            for each_path, each_file in self.last_local_index.items()
            if each_path not in local_index and each_path not in self.deleted_locally}

        remotely_removed = {
            each_path: each_file
            for each_path, each_file in self.last_remote_index.items()
            if each_path not in remote_index and each_path not in self.deleted_remotely}

        return Delta(locally_modified, locally_removed), Delta(remotely_modified, remotely_removed)

    def _sync_action(self, index_src: FILE_INDEX, index_dst: FILE_INDEX, method: SyncAction, direction: SyncDirection) -> FILE_INDEX:
        if direction == SyncDirection.UP:
            if method == SyncAction.DEL:
                action = self._method_delete_remote
            elif method == SyncAction.ADD:
                action = self._method_upload
            else:
                raise Exception("Invalid method")

        elif direction == SyncDirection.DOWN:
            if method == SyncAction.DEL:
                action = self._method_delete_local
            elif method == SyncAction.ADD:
                action = self._method_download
            else:
                raise Exception("Invalid method")

        else:
            raise Exception("Invalid direction")

        action_cache = dict()
        for each_path, src_file in index_src.items():
            dst_file = index_dst.get(each_path)
            if dst_file is None:
                if method == SyncAction.ADD:
                    action_cache[each_path] = src_file

                elif method == SyncAction.DEL:
                    continue

            elif method == SyncAction.ADD:
                if dst_file == src_file:
                    continue

                elif dst_file.timestamp < src_file.timestamp:
                    action_cache[each_path] = src_file

                else:
                    self.main_logger.warning(f"Conflict {method:s} {each_path:s} {direction:s}: source is older than target.")
                    continue

            elif method == SyncAction.DEL:
                if dst_file == src_file:
                    action_cache[each_path] = src_file

                else:
                    self.main_logger.warning(f"Conflict {method:s} {each_path:s} {direction:s}: unexpected target.")
                    continue

        action(action_cache)
        return action_cache

    @staticmethod
    def _dropbox_path_format(path: pathlib.PurePosixPath) -> str:
        posix = path.as_posix()
        if posix.startswith("/"):
            return posix[1:]
        return posix

    def _get_time_offset(self: DropboxSync) -> float:
        """Upload an empty file to get the time offset between the local and remote system."""
        tmp_name = ".time_offset"

        tmp_file = self.local_folder / tmp_name
        tmp_file.touch(exist_ok=True)
        stat = tmp_file.stat()
        local_time = stat.st_mtime

        remote_path = DropboxSync._dropbox_path_format(self.dropbox_folder / tmp_name)

        with tmp_file.open(mode="rb") as file:
            entry = self.client.files_upload(file.read(), remote_path, mode=db_files.WriteMode("overwrite"))
            remote_time = entry.server_modified.timestamp()

        tmp_file.unlink()
        self.client.files_delete_v2(remote_path)
        return round(local_time - remote_time, 2)

    def sync(self: DropboxSync) -> None:
        self.time_offset = self._get_time_offset()
        self.main_logger.info(f"Time offset: {self.time_offset:.2f} ms")

        local_index = self._get_local_index()
        remote_index = self._get_remote_index()

        local_delta, remote_delta = self._get_deltas(local_index, remote_index)

        # modifying works, creating works, deletion does not
        self.uploaded = self._sync_action(local_delta.modified, remote_index, SyncAction.ADD, SyncDirection.UP)
        self.deleted_remotely = self._sync_action(local_delta.deleted, remote_index, SyncAction.DEL, SyncDirection.UP)
        self.downloaded = self._sync_action(remote_delta.modified, local_index, SyncAction.ADD, SyncDirection.DOWN)
        self.deleted_locally = self._sync_action(remote_delta.deleted, local_index, SyncAction.DEL, SyncDirection.DOWN)

        self.last_local_index.clear()
        local_index.update(self.downloaded)
        for each_path in self.deleted_locally:
            local_index.pop(each_path)
        self.last_local_index.update(local_index)

        self.last_remote_index.clear()
        remote_index.update(self.uploaded)
        for each_path in self.deleted_remotely:
            remote_index.pop(each_path)
        self.last_remote_index.update(remote_index)


def main() -> None:
    config_path = "config.json"
    config = DropboxSync.get_config(config_path)

    db_sync = DropboxSync(**config)

    while True:
        db_sync.sync()
        time.sleep(db_sync.interval_seconds)


if __name__ == "__main__":
    main()
