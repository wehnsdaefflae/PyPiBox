import hashlib
import mmap
from pathlib import Path

DROPBOX_HASH_CHUNK_SIZE = 4 * 1024 * 1024


def compute_dropbox_hash(path: Path) -> str:
    # https://stackoverflow.com/questions/13008040/locally-calculate-dropbox-hash-of-files
    with path.open(mode='rb') as f:
        block_hashes = b''
        while True:
            chunk = f.read(DROPBOX_HASH_CHUNK_SIZE)
            if not chunk:
                break
            block_hashes += hashlib.sha256(chunk).digest()
        return hashlib.sha256(block_hashes).hexdigest()


def compute_dropbox_hash_memory_mapped(path: Path) -> str:
    if not path.is_file():
        raise ValueError("Path must point to a file")

    block_hashes = bytearray()
    file_size = path.stat().st_size

    if file_size < 1:
        # hashlib.sha256(block_hashes).hexdigest()
        # hashlib.sha256(b"").hexdigest()
        return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    with path.open(mode='rb') as f:
        with mmap.mmap(f.fileno(), length=file_size, access=mmap.ACCESS_READ) as memory_mapped_file:
            for chunk_start in range(0, file_size, DROPBOX_HASH_CHUNK_SIZE):
                chunk_end = min(chunk_start + DROPBOX_HASH_CHUNK_SIZE, file_size)
                chunk = memory_mapped_file[chunk_start:chunk_end]
                block_hashes.extend(hashlib.sha256(chunk).digest())

    return hashlib.sha256(block_hashes).hexdigest()
