import time
import uuid
from dataclasses import dataclass

from trinitylake.storage import Storage
from trinitylake.protobuf import LakehouseDef, NamespaceDef, TableDef
from trinitylake.storage import file_paths
from trinitylake.tree import TrinityTree, TrinityNode
from trinitylake.const import LAKEHOUSE_DEF, VERSION, LATEST_HINT_FILE
from trinitylake.storage import tree_operations


@dataclass
class RunningTransaction:
    transaction_id: str
    beginning_root: TrinityTree
    running_root: TrinityTree
    began_at_millis: int


class TrinityLake:
    def create_lakehouse(self, storage: Storage, lakehouse_def: LakehouseDef):
        lakehouse_def_path = file_paths.new_lakehouse_def_path()
        storage.serialize_protobuf(lakehouse_def, lakehouse_def_path)

        root_node = TrinityNode()
        root_file_path = file_paths.get_root_node_path(0)
        root_node.system_rows.append((LAKEHOUSE_DEF, lakehouse_def_path))
        root_node.system_rows.append((VERSION, "0"))
        storage.write_node(root_node, root_file_path)

        storage.write_file(LATEST_HINT_FILE, str(0).encode("utf-8"))

    def begin_transaction(self, storage: Storage, options=None):
        tree = tree_operations.find_latest_root(storage)

        return RunningTransaction(
            transaction_id=str(uuid.uuid4()),
            beginning_root=tree,
            running_root=tree,
            began_at_millis=int(time.time() * 1000),
        )

    def commit_transaction(self):
        pass

    def rollback_transaction(self):
        pass

    def show_namespaces(self, transaction: RunningTransaction):
        pass


