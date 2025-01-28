from const import LATEST_HINT_FILE
from trinitylake.lakehouse_version import LakehouseVersion


class Lakehouse:

    def __init__(self, storage):
        self.storage = storage

    def begin_transaction(self) -> LakehouseVersion:
        current_version = self._resolve_latest_version()
        tree_path = f"{current_version:032b}.ipc"
        return LakehouseVersion(current_version, self.storage, tree_path)

    def commit_transaction(self, lakehouse_version: LakehouseVersion):
        version_id = lakehouse_version.get_version()
        new_version = version_id + 1
        trinity_tree = lakehouse_version.tree
        root_node = trinity_tree.root
        # for change in changes:
        #     if not change[1]:
        #         trinity_tree.delete(change[0])
        #     else:
        #         trinity_tree.insert(change[0], change[1])
        # TODO: update system rows
        tree_name = f"{new_version:032b}.ipc"
        self.storage.serialize_tree(root_node, tree_name)
        self.storage.write_file(LATEST_HINT_FILE, str(new_version).encode("utf-8"))

    def rollback_transaction(self, transaction_id):
        pass

    def _resolve_latest_version(self):
        version_hint = 0
        if self.storage.exists(LATEST_HINT_FILE):
            version_hint = int(self.storage.read_file(LATEST_HINT_FILE).decode())
            return version_hint

        current_version = version_hint
        while True:
            next_version = current_version + 1
            next_root = f"{next_version:032b}.ipc"
            if not self.storage.exists(next_root):
                break
            current_version = next_version

        return current_version
