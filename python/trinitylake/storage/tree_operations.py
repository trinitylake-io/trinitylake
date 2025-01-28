from const import IPC_FILE_SUFFIX
from tree import TrinityTree
from trinitylake.storage import Storage
from trinitylake.const import LATEST_HINT_FILE


def find_latest_root(storage: Storage) -> TrinityTree:
    latest_version = find_latest_version(storage)
    tree_path = f"{latest_version:032b}.ipc"
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
        next_root = f"{next_version:032b}{IPC_FILE_SUFFIX}"
        if not storage.exists(next_root):
            break
        current_version = next_version
    return current_version
