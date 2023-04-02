import random
import pathlib
import time


def main():
    local_files = set()
    remote_files = set()

    local_folder = pathlib.Path("")
    remote_folder = pathlib.Path("")

    for file_index in range(1_000):
        delete = random.random() < .5
        local = random.random() < .5

        file_name = f"test_file_{file_index:08d}.txt"

        if delete and local and len(local_files) >= 1:
            file_name = random.choice(list(local_files))
            file_path = local_folder / file_name
            local_files.remove(file_path)
            file_path.unlink()

        elif delete and not local and len(remote_files) >= 1:
            file_name = random.choice(list(remote_files))
            file_path = remote_folder / file_name
            remote_files.remove(file_path)
            file_path.unlink()

        elif local:
            file_path = local_folder / file_name
            local_files.add(file_name)
            file_path.touch()

        else:
            file_path = remote_folder / file_name
            remote_files.add(file_name)
            file_path.touch()

        time.sleep(random.random())
        if file_index % 100 == 0:
            # check is sync
            pass

    # check is sync


if __name__ == '__main__':
    main()
