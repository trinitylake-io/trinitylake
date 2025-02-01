from const import IPC_FILE_SUFFIX
from trinitylake.tree import TrinityTree
from trinitylake.storage import Storage
from trinitylake.const import LATEST_HINT_FILE
from trinitylake.storage import file_paths


def find_latest_root(storage: Storage) -> TrinityTree:
    latest_version = find_latest_version(storage)
    tree_path = file_paths.get_root_node_path(latest_version)
    root_node = storage.deserialize_tree(tree_path)
    return TrinityTree(root_node)


def find_latest_version(storage: Storage):
    version_hint = 0
    if storage.exists(LATEST_HINT_FILE):
        version_hint = int(storage.read_file(LATEST_HINT_FILE).decode())
        return version_hint
    current_version = version_hint
    while True:
        next_version = current_version + 1
        next_root = file_paths.get_root_node_path(next_version)
        if not storage.exists(next_root):
            break
        current_version = next_version
    return current_version
