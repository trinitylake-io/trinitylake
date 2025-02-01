import time
import uuid
from dataclasses import dataclass

from const import HIGHEST_CHARACTER_VALUE
from trinitylake.storage import Storage
from trinitylake.protobuf import LakehouseDef, NamespaceDef, TableDef
from trinitylake.storage import file_paths
from trinitylake.tree import TrinityTree, TrinityNode, tree_keys
from trinitylake.const import (
    LAKEHOUSE_DEF,
    VERSION,
    LATEST_HINT_FILE,
    NAMESPACE_SCHEMA_ID_PART,
    TABLE_SCHEMA_ID_PART,
)
from trinitylake.storage import tree_operations


@dataclass
class RunningTransaction:
    transaction_id: str
    beginning_tree: TrinityTree
    running_tree: TrinityTree
    began_at_millis: int


def create_lakehouse(storage: Storage, lakehouse_def: LakehouseDef):
    lakehouse_def_path = file_paths.new_lakehouse_def_path()
    storage.serialize_protobuf(lakehouse_def, lakehouse_def_path)

    root_node = TrinityNode()
    root_file_path = file_paths.get_root_node_path(0)
    root_node.system_rows.append((LAKEHOUSE_DEF, lakehouse_def_path))
    root_node.system_rows.append((VERSION, "0"))
    storage.write_node(root_node, root_file_path)

    storage.write_file(LATEST_HINT_FILE, str(0).encode("utf-8"))


def begin_transaction(storage: Storage):
    tree = tree_operations.find_latest_root(storage)

    return RunningTransaction(
        transaction_id=str(uuid.uuid4()),
        beginning_tree=tree,
        running_tree=tree,
        began_at_millis=int(time.time() * 1000),
    )


def commit_transaction(storage: Storage, transaction: RunningTransaction):
    version = int(_find_version(transaction.running_tree))
    root_node = transaction.running_tree.root
    new_version = version + 1
    # for change in changes:
    #     if not change[1]:
    #         trinity_tree.delete(change[0])
    #     else:
    #         trinity_tree.insert(change[0], change[1])
    # TODO: update system rows
    tree_name = file_paths.get_root_node_path(new_version)
    storage.serialize_tree(root_node, tree_name)
    storage.write_file(LATEST_HINT_FILE, str(new_version).encode("utf-8"))


def show_namespaces(transaction: RunningTransaction):
    running_root = transaction.running_tree
    return [
        tree_keys.get_namespace_name_from_key(k)
        for k, v in running_root.range_query(
            NAMESPACE_SCHEMA_ID_PART, NAMESPACE_SCHEMA_ID_PART + HIGHEST_CHARACTER_VALUE
        )
    ]


def create_namespace(
    storage: Storage,
    transaction: RunningTransaction,
    namespace_name: str,
    namespace_def: NamespaceDef,
):
    lakehouse_def = _find_lakehouse_def(storage, transaction.running_tree)
    namespace_key = tree_keys.get_namespace_key(namespace_name, lakehouse_def)
    try:
        transaction.running_tree.get(namespace_key)
    except KeyError:
        pass
    file_path = storage.serialize_protobuf(namespace_def)
    transaction.running_tree.insert(namespace_key, file_path)
    return transaction


def describe_namespace(
    storage: Storage, transaction: RunningTransaction, namespace_name: str
):
    lakehouse_def = _find_lakehouse_def(storage, transaction.running_tree)
    namespace_key = tree_keys.get_namespace_key(namespace_name, lakehouse_def)

    try:
        namespace_def = transaction.running_tree.get(namespace_key)
    except KeyError:
        raise ValueError(f"Namespace {namespace_name} does not exist")
    return storage.deserialize_protobuf(namespace_def, NamespaceDef)


def drop_namespace(
    storage: Storage, transaction: RunningTransaction, namespace_name: str
):
    lakehouse_def = _find_lakehouse_def(storage, transaction.running_tree)
    namespace_key = tree_keys.get_namespace_key(namespace_name, lakehouse_def)
    try:
        transaction.running_tree.get(namespace_key)
    except KeyError:
        raise ValueError(f"Namespace {namespace_name} does not exist")
    transaction.running_tree.delete(namespace_key)


def show_tables(transaction: RunningTransaction, namespace_name: str):
    running_root = transaction.running_tree
    start = f"{TABLE_SCHEMA_ID_PART}{namespace_name}"
    end = f"{start} {HIGHEST_CHARACTER_VALUE}"
    return [
        tree_keys.get_table_name_from_key(namespace_name, k)
        for k, v in running_root.range_query(start, end)
    ]


def describe_table(
    storage: Storage,
    transaction: RunningTransaction,
    namespace_name: str,
    table_name: str,
):
    lakehouse_def = _find_lakehouse_def(storage, transaction.running_tree)
    table_key = tree_keys.get_table_key(namespace_name, table_name, lakehouse_def)
    try:
        table_def = transaction.running_tree.get(table_key)
    except KeyError:
        raise ValueError(f"Table {table_name} does not exist")
    return storage.deserialize_protobuf(table_def, TableDef)


def create_table(
    storage: Storage,
    transaction: RunningTransaction,
    namespace_name: str,
    table_name: str,
    table_def: TableDef,
):
    lakehouse_def = _find_lakehouse_def(storage, transaction.running_tree)
    table_key = tree_keys.get_table_key(namespace_name, table_name, lakehouse_def)
    try:
        transaction.running_tree.get(table_key)
        raise ValueError(f"Table {table_name} already exists")
    except KeyError:
        pass
    file_path = storage.serialize_protobuf(table_def)
    transaction.running_tree.insert(table_key, file_path)
    return transaction


def _find_lakehouse_def(storage: Storage, tree: TrinityTree):
    for sys_key in tree.root.system_rows:
        if sys_key[0] == LAKEHOUSE_DEF:
            return storage.deserialize_protobuf(sys_key[1], LakehouseDef)
    raise ValueError("No lakehouse definition found")


def _find_version(tree: TrinityTree):
    for sys_key in tree.root.system_rows:
        if sys_key[0] == VERSION:
            return sys_key[1]
    raise ValueError("No version found")
