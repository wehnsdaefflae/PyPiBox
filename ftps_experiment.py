import os
from ftplib import FTP_TLS
from typing import Tuple, List, Union
import datetime


def connect_ftps(host: str, username: str, password: str) -> FTP_TLS:
    ftps = FTP_TLS()
    ftps.connect(host)
    ftps.login(username, password)
    ftps.prot_p()
    return ftps


def parse_list_line(line: str) -> Tuple[str, datetime.datetime, bool, Union[int, None]]:
    parts = line.split()
    timestamp = datetime.datetime.strptime(parts[0] + ' ' + parts[1], "%Y%m%d %H%M%S")
    is_directory = parts[2] == '<dir>'
    size = int(parts[3]) if not is_directory else None
    name = ' '.join(parts[4:])
    return name, timestamp, is_directory, size


def get_recursive_files_dirs(ftps: FTP_TLS, directory: str) -> List[Tuple[str, datetime.datetime, Union[int, None]]]:
    items = []

    def process_item(_name: str, _timestamp: datetime.datetime, _is_directory: bool, _size: Union[int, None], _item_path: str):
        if _is_directory:
            items.append((_item_path + '/', _timestamp, None))
            _lines = []
            _retrlines_callback = lambda _line: _lines.append(_line)
            ftps.retrlines(f"LIST {_item_path}", _retrlines_callback)
            for _each_line in _lines:
                sub_name, sub_timestamp, sub_is_directory, sub_size = parse_list_line(_each_line)
                subitem_path = os.path.join(_item_path, sub_name)
                # process_item(sub_name, sub_timestamp, sub_is_directory, sub_size, subitem_path)
        else:
            items.append((_item_path, _timestamp, _size))

    retrlines_callback = lambda _line: lines.append(_line)
    lines = []
    ftps.retrlines(f"LIST {directory}", retrlines_callback)

    for line in lines:
        name, timestamp, is_directory, size = parse_list_line(line)
        item_path = os.path.join(directory, name)
        process_item(name, timestamp, is_directory, size, item_path)

    return items


def main():
    host = "192.168.10.1"
    username = "mark"
    password = "6bB3gv%2zeF4wxJ("
    directory = "/"

    ftps = connect_ftps(host, username, password)
    items = get_recursive_files_dirs(ftps, directory)
    ftps.quit()

    for entry in items:
        print(entry)


if __name__ == "__main__":
    main()
