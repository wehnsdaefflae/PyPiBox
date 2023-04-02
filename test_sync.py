import random
import pathlib
import time


def make_file(file_name: str) -> None:
    raise NotImplemented()


def get_hash(path: pathlib.Path) -> str:
    raise NotImplemented()


def generate_random_string() -> str:
    raise NotImplemented()


def directories_in_sync(directory_a: pathlib.Path, directory_b: pathlib.Path) -> bool:
    # check directory names, file names, and file hashes
    raise NotImplemented()


def directory_up_to_date(directory: pathlib.Path, index: dict[str, str]) -> bool:
    # check names and hashes
    raise NotImplemented()


def main():
    local_files = dict()
    remote_files = dict()

    local_folder = pathlib.Path("")
    remote_folder = pathlib.Path("")

    for file_index in range(1_000):
        # create new file locally
        # create new file remotely
        # modify file locally
        # modify file remotely
        # delete file locally
        # delete file remotely


        time.sleep(random.random())
        if file_index % 100 == 0:
            # check is sync
            pass

    # check is sync


if __name__ == '__main__':
    main()
