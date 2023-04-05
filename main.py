from __future__ import annotations
import json
import os
import pathlib
import time
from typing import Optional

import dropbox
from dropbox import files as db_files, exceptions as db_exceptions
from dropbox.files import DeleteArg

import logging

from utils import FileInfo, FILE_INDEX, compute_dropbox_hash, get_mod_time_locally, get_mod_time_remotely, depth, \
    Delta

from utils import SyncDirection, SyncAction


class DropboxSync:
    def __init__(self: DropboxSync, config_path: str) -> None:
        with open(config_path, mode="r") as file:
            config = json.load(file)

        refresh_token = config.get("refresh_token", "")
        if len(refresh_token) < 1:
            authorization_flow = dropbox.DropboxOAuth2FlowNoRedirect(
                config["app_key"],
                consumer_secret=config["app_secret"],
                token_access_type="offline")\

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

        self.client = dropbox.Dropbox(
            app_key=config["app_key"],
            app_secret=config["app_secret"],
            oauth2_refresh_token=config["refresh_token"])

        self.client.check_and_refresh_access_token()
        self.interval_seconds = config.get("interval_seconds", 60)
        self.local_folder = config["local_folder"]
        assert self.local_folder[-1] == "/"
        os.makedirs(self.local_folder, exist_ok=True)

        self.dropbox_folder = config["dropbox_folder"]
        assert self.dropbox_folder[-1] == "/"
        if len(self.dropbox_folder) < 2:
            self.dropbox_folder = ""

        self.log_file = os.path.join("events.log")
        logging.basicConfig(filename=self.log_file, level=logging.INFO)

        self.uploaded, self.downloaded = dict(), dict()
        self.deleted_remotely, self.deleted_locally = dict(), dict()

        self.last_local_index = dict()
        self.last_remote_index = dict()

        self.time_offset = -1.

    def _get_local_index(self: DropboxSync) -> FILE_INDEX:
        logging.info("Getting local index...")
        local_file_index = dict()
        length = len(self.local_folder)
        for root, dirs, file_paths in os.walk(self.local_folder):
            for each_path in file_paths:
                full_path = os.path.join(root, each_path)
                mod_dt = get_mod_time_locally(full_path)
                db_hash = compute_dropbox_hash(full_path)
                each_file = FileInfo(full_path[length:], mod_dt, db_hash)
                local_file_index[each_file.path] = each_file

            for each_path in dirs:
                full_path = os.path.join(root, each_path)
                mod_dt = get_mod_time_locally(full_path)
                each_dir = FileInfo(f"{full_path[length:]:s}/", mod_dt, None)
                local_file_index[each_dir.path] = each_dir

        return local_file_index

    def _get_remote_index(self: DropboxSync) -> FILE_INDEX:
        logging.info("Getting remote index...")
        remote_index = dict()
        length = len(self.dropbox_folder)
        result = self.client.files_list_folder(self.dropbox_folder, recursive=True)

        while True:
            for entry in result.entries:
                if isinstance(entry, db_files.FileMetadata):
                    mod_dt = get_mod_time_remotely(entry, offset=self.time_offset)
                    db_hash = entry.content_hash
                    each_file = FileInfo(entry.path_display[length:], mod_dt, db_hash)
                    remote_index[each_file.path] = each_file

                elif isinstance(entry, db_files.FolderMetadata):
                    truncated = entry.path_display[length:]
                    if 0 < len(truncated):
                        each_dir = FileInfo(f"{truncated:s}/", -1., None)
                        remote_index[each_dir.path] = each_dir

            if not result.has_more:
                break

            result = self.client.files_list_folder_continue(result.cursor)

        return remote_index

    def _method_upload(self: DropboxSync, file_index: FILE_INDEX) -> None:
        len_paths = len(file_index)
        if len_paths < 1:
            return
        logging.info(f"Uploading {len_paths:d} files")

        for each_path_tail_slash, each_file in file_index.items():
            each_path = each_file.path_no_tail_slash
            dst_path = self.dropbox_folder + each_path
            remote_file = self._get_remote_file(each_path)

            if each_file.is_folder:
                if remote_file is not None and remote_file.is_folder:
                    continue

                self.client.files_create_folder_v2(dst_path)
                continue

            # necessary in case of intermediate upload from somewhere else!
            if remote_file is not None and not remote_file.is_folder and not remote_file.is_deleted:
                if remote_file.hash == each_file.hash:
                    continue

                if remote_file.timestamp >= each_file.timestamp:
                    logging.warning(f"Conflict: More recent remote file {each_path:s}. Skipping...")
                    continue

            src_path = self.local_folder + each_file.path
            stats = os.stat(src_path)
            if stats.st_size > 150 * 1024 * 1024:
                # self.client.files_upload_session_start(open(src_path, "rb").read())
                # self.client.files_upload_session_finish(open(src_path, "rb").read())
                # todo: use `files_upload_session_start` instead
                msg = f"File {each_path:s} is too large. Skipping..."
                logging.critical(msg)
                raise ValueError(msg)

            with open(src_path, mode="rb") as file:
                self.client.files_upload(file.read(), dst_path, mode=db_files.WriteMode("overwrite"))
                # https://github.com/dropbox/dropbox-sdk-python/blob/master/example/updown.py
                # self.client.files_upload_session_start()

    def _method_download(self: DropboxSync, file_index: FILE_INDEX) -> None:
        len_paths = len(file_index)
        if len(file_index) < 1:
            return
        logging.info(f"Downloading {len_paths:d} files")

        directories = [each_path for each_path, each_file in file_index.items() if each_file.is_folder]
        directories.sort(key=depth)
        for each_path in directories:
            local_path = self.local_folder + each_path
            if os.path.exists(local_path):
                continue

            os.mkdir(local_path)

        for each_path, each_file in file_index.items():
            if each_file.is_folder:
                continue
            remote_path = self.dropbox_folder + each_path
            local_path = self.local_folder + each_path
            if os.path.exists(local_path):
                local_hash = compute_dropbox_hash(local_path)
                if local_hash == each_file.hash:
                    continue

                local_time = get_mod_time_locally(local_path)
                if local_time >= each_file.timestamp:
                    logging.warning(f"Conflict: More recent local file {each_path:s}. Skipping...")
                    continue

            self.client.files_download_to_file(local_path, remote_path)
            # os.utime(local_path, (each_file.timestamp, each_file.timestamp))

    def _method_delete_remote(self: DropboxSync, file_index: FILE_INDEX) -> None:
        len_paths = len(file_index)
        if len_paths < 1:
            return
        logging.warning(f"Deleting {len_paths:d} remote files")

        file_entries = [
            DeleteArg(self.dropbox_folder + each_file.path)
            for each_file in file_index.values()
            if not each_file.is_folder]
        self.client.files_delete_batch(file_entries)

        folders = [each_file.path_no_tail_slash for _, each_file in file_index.items() if each_file.is_folder]
        folders.sort(key=depth)
        deleted = set()
        delete_args = []
        for each_path in folders:
            if any(each_path.startswith(each_deleted) for each_deleted in deleted):
                continue
            delete_args.append(DeleteArg(self.dropbox_folder + each_path))
            deleted.add(each_path)

        for i in range(0, len(delete_args), 1000):
            self.client.files_delete_batch(delete_args[i:i + 1000])

        """ simple variant             
        folders.sort(key=depth, reverse=True)
        folder_entries = [DeleteArg(self.dropbox_folder + each_path) for each_path in folders]
        self.client.files_delete_batch(folder_entries)
        """

    def _method_delete_local(self: DropboxSync, file_index: FILE_INDEX) -> None:
        len_paths = len(file_index)
        if len_paths < 1:
            return
        logging.warning(f"Deleting {len_paths:d} local files")

        for each_path, each_file in file_index.items():
            if not each_file.is_folder:
                path = self.local_folder + each_path
                os.remove(path)

        folders = [each_path for each_path, each_file in file_index.items() if each_file.is_folder]
        folders.sort(key=depth, reverse=True)
        for each_path in folders:
            path = self.local_folder + each_path
            os.rmdir(path)

    def _get_remote_file(self, each_path: str) -> Optional[FileInfo]:
        try:
            entry = self.client.files_get_metadata(self.dropbox_folder + each_path)
            if isinstance(entry, db_files.FolderMetadata):
                return FileInfo(f"{each_path}/", -1., None)

            if isinstance(entry, db_files.FileMetadata):
                return FileInfo(each_path, get_mod_time_remotely(entry, offset=self.time_offset), entry.content_hash)

        except db_exceptions.ApiError as err:
            if err.error.is_path() and err.error.get_path().is_not_found():
                return None
            else:
                raise err

    def __get_remote_delta(self: DropboxSync) -> tuple[FILE_INDEX, FILE_INDEX]:
        raise Exception("Does not work.")

        created = dict()
        removed = dict()

        result = self.client.files_list_folder_get_latest_cursor(
            self.dropbox_folder, recursive=True, include_deleted=True)

        while True:
            result = self.client.files_list_folder_continue(result.cursor)

            for each_entry in result.entries:
                if isinstance(each_entry, db_files.FolderMetadata):
                    mod_dt = get_mod_time_remotely(each_entry, offset=self.time_offset)
                    each_dir = FileInfo(f"{each_entry.path_display}/", mod_dt, None)
                    created[each_dir.path] = each_dir

                elif isinstance(each_entry, db_files.FileMetadata):
                    mod_dt = get_mod_time_remotely(each_entry, offset=self.time_offset)
                    db_hash = each_entry.content_hash
                    each_file = FileInfo(each_entry.path_display, mod_dt, db_hash)
                    created[each_file.path] = each_file

                elif isinstance(each_entry, db_files.DeletedMetadata):
                    each_path = FileInfo(each_entry.path_display, -1., None)
                    removed[each_path.path] = each_path

            if not result.has_more:
                break

        return created, removed

    @staticmethod
    def _different_hash(file: FileInfo, last_index: FILE_INDEX) -> bool:
        file_info = last_index.get(file.path)
        if file_info is None:
            return True
        return file.hash != file_info.hash  # or file.timestamp != last_index[file.path].timestamp

    def _get_deltas(self, local_index: FILE_INDEX, remote_index: FILE_INDEX) -> tuple[Delta, Delta]:
        locally_modified = {
            each_path: each_file
            for each_path, each_file in local_index.items()
            if (DropboxSync._different_hash(each_file, self.last_local_index) and each_path not in self.downloaded)}

        remotely_modified = {
            each_path: each_file
            for each_path, each_file in remote_index.items()
            if (DropboxSync._different_hash(each_file, self.last_remote_index) and each_path not in self.uploaded)}

        locally_removed = {
            each_path: each_file
            for each_path, each_file in self.last_local_index.items()
            if each_path not in local_index and each_path not in self.deleted_locally}

        remotely_removed = {
            each_path: each_file
            for each_path, each_file in self.last_remote_index.items()
            if each_path not in remote_index and each_path not in self.deleted_remotely}

        return Delta(locally_modified, locally_removed), Delta(remotely_modified, remotely_removed)

    def _sync_action(self, index_src: FILE_INDEX, index_dst: FILE_INDEX, method: SyncAction,
                     direction: SyncDirection) -> FILE_INDEX:
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
                if dst_file.hash == src_file.hash:
                    continue

                elif dst_file.timestamp < src_file.timestamp:
                    action_cache[each_path] = src_file

                else:
                    logging.warning(f"Conflict {method:s} {each_path:s} {direction:s}: source is older than target.")
                    continue

            elif method == SyncAction.DEL:
                if dst_file.hash == src_file.hash:
                    action_cache[each_path] = src_file

                else:
                    logging.warning(f"Conflict {method:s} {each_path:s} {direction:s}: unexpected target.")
                    continue

        action(action_cache)
        return action_cache

    def _get_time_offset(self: DropboxSync) -> float:
        """Upload an empty file to get the time offset between the local and remote system."""
        tmp_name = ".time_offset"

        tmp_file = pathlib.Path(self.local_folder) / tmp_name
        tmp_file.touch(exist_ok=True)
        stat = tmp_file.stat()
        local_time = stat.st_mtime

        remote_path = self.dropbox_folder + tmp_name

        with tmp_file.open(mode="rb") as file:
            entry = self.client.files_upload(file.read(), remote_path, mode=db_files.WriteMode("overwrite"))
            remote_time = entry.server_modified.timestamp()

        tmp_file.unlink()
        self.client.files_delete_v2(remote_path)
        return local_time - remote_time

    def sync(self: DropboxSync) -> None:
        self.time_offset = self._get_time_offset()

        local_index = self._get_local_index()
        remote_index = self._get_remote_index()

        local_delta, remote_delta = self._get_deltas(local_index, remote_index)

        # modifying works, creating works, deletion does not
        self.uploaded = self._sync_action(local_delta.modified, remote_index, SyncAction.ADD, SyncDirection.UP)
        self.deleted_remotely = self._sync_action(local_delta.deleted, remote_index, SyncAction.DEL, SyncDirection.UP)
        self.downloaded = self._sync_action(remote_delta.modified, local_index, SyncAction.ADD, SyncDirection.DOWN)
        self.deleted_locally = self._sync_action(remote_delta.deleted, local_index, SyncAction.DEL, SyncDirection.DOWN)

        self.last_local_index.clear()
        self.last_local_index.update(local_index)

        self.last_remote_index.clear()
        self.last_remote_index.update(remote_index)


def main() -> None:
    db_sync = DropboxSync("config.json")

    while True:
        db_sync.sync()
        time.sleep(db_sync.interval_seconds)


if __name__ == "__main__":
    main()
