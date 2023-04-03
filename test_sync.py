import os
import random
import pathlib
import time
import networkx


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


def find_leaves(tree):
    leaves = [node for node in tree.nodes() if tree.degree(node) == 1 and node != 0]
    return leaves


def leaf_paths(tree, leaves):
    root = 0  # Assuming the root node is 0, change it if required
    for leaf in leaves:
        yield networkx.shortest_path(tree, source=root, target=leaf)


def main():
    local_folder = pathlib.Path("./local/")
    num_nodes = 50  # Change the number of nodes in the tree as required
    tree = networkx.random_tree(num_nodes)
    leaves = find_leaves(tree)
    directories = []
    for each_path in leaf_paths(tree, leaves):
        directories.append(local_folder / pathlib.Path(*(str(x) for x in each_path[1:])))

    directories.sort(key=lambda x: x.as_posix().count("/"))
    for each_path in directories:
        each_path.mkdir(parents=True, exist_ok=True)

    files = (each_path.with_suffix(".txt") for each_path in directories)
    for each_path in files:
        each_path.touch()

    exit()

    structure = networkx.random_tree(10, create_using=networkx.DiGraph, seed=42)
    directories = networkx.dag_to_branching(structure)
    for each_edge in directories:
        print(each_edge)

    exit()
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
