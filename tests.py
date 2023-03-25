import time
from pathlib import Path

from dropbox_content_hasher import db_hash
from main import get_all_local_files
from new_hasher import db_hash as new_db_hash
from windy_hasher import compute_dropbox_hash as windy_db_hash, \
    compute_dropbox_hash_memory_mapped as windy_db_hash_memory_mapped


def identical() -> None:
    entries_a = get_all_local_files(Path("/home/mark/Dropbox/workspace/"), hasher=db_hash)
    entries_b = get_all_local_files(Path("/home/mark/Dropbox/workspace/"), hasher=new_db_hash)
    entries_c = get_all_local_files(Path("/home/mark/Dropbox/workspace/"), hasher=windy_db_hash)
    entries_d = get_all_local_files(Path("/home/mark/Dropbox/workspace/"), hasher=windy_db_hash_memory_mapped)

    for each_entry, each_hash in entries_a.items():
        if each_entry not in entries_b:
            print(f"Entry {each_entry.as_posix():s} not found in entries_b")
            continue

        if each_entry not in entries_c:
            print(f"Entry {each_entry.as_posix():s} not found in entries_c")
            continue

        if each_entry not in entries_d:
            print(f"Entry {each_entry.as_posix():s} not found in entries_d")
            continue

        if each_hash != entries_b[each_entry]:
            print(f"Hash mismatch for {each_entry.as_posix():s} in entries_b")
            continue

        if each_hash != entries_c[each_entry]:
            print(f"Hash mismatch for {each_entry.as_posix():s} in entries_c")
            continue

        if each_hash != entries_d[each_entry]:
            print(f"Hash mismatch for {each_entry.as_posix():s} in entries_d")
            continue

    print("All entries match")


def timing() -> None:
    t = time.time()
    for i in range(10):
        entries_a = get_all_local_files(Path("/home/mark/Dropbox/workspace/"), hasher=db_hash)
    print(time.time() - t)

    t = time.time()
    for i in range(10):
        entries_b = get_all_local_files(Path("/home/mark/Dropbox/workspace/"), hasher=new_db_hash)
    print(time.time() - t)

    t = time.time()
    for i in range(10):
        entries_c = get_all_local_files(Path("/home/mark/Dropbox/workspace/"), hasher=windy_db_hash)
    print(time.time() - t)

    t = time.time()
    for i in range(10):
        entries_d = get_all_local_files(Path("/home/mark/Dropbox/workspace/"), hasher=windy_db_hash_memory_mapped)
    print(time.time() - t)


def main() -> None:
    identical()
    timing()
