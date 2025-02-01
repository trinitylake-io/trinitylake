import pytest
import tempfile

from const import LAKEHOUSE_DEF, VERSION, LATEST_HINT_FILE
from storage import file_paths
from trinity_lake import RunningTransaction
from trinitylake.storage.storage import Storage
from trinitylake import trinity_lake
from trinitylake.protobuf import LakehouseDef, NamespaceDef, TableDef

@pytest.fixture
def storage():
    temp_dir = tempfile.mkdtemp()
    print(f"Using temp dir: {temp_dir}")
    return Storage(temp_dir)

@pytest.fixture
def lakehouse_def():
    lakehouse = LakehouseDef()
    lakehouse.major_version = 0
    lakehouse.order = 128
    lakehouse.namespace_name_max_size_bytes = 100
    lakehouse.table_name_max_size_bytes = 100
    lakehouse.file_path_max_size_bytes = 200
    lakehouse.node_file_max_size_bytes = 1048576
    lakehouse.minimum_versions_to_keep = 3
    lakehouse.maximum_version_age_millis = 604800000
    return lakehouse

@pytest.fixture
def storage_with_lakehouse(storage, lakehouse_def):
    trinity_lake.create_lakehouse(storage, lakehouse_def)
    return storage

def test_create_lakehouse(storage, lakehouse_def):
    trinity_lake.create_lakehouse(storage, lakehouse_def)

    assert storage.exists(file_paths.get_root_node_path(0))
    txn = trinity_lake.begin_transaction(storage)

    system_row_keys = [k for k, v in txn.beginning_tree.root.system_rows]
    assert LAKEHOUSE_DEF in system_row_keys
    assert VERSION in system_row_keys

    assert storage.read_file(LATEST_HINT_FILE) == b"0"

def test_create_and_describe_namespace(storage_with_lakehouse):
    transaction = trinity_lake.begin_transaction(storage_with_lakehouse)
    namespace_name = "default"
    namespace_def = NamespaceDef()

    trinity_lake.create_namespace(storage_with_lakehouse, transaction, namespace_name, namespace_def)
    described = trinity_lake.describe_namespace(storage_with_lakehouse, transaction, namespace_name)

    assert described == namespace_def


def test_drop_namespace(storage_with_lakehouse):
    transaction = trinity_lake.begin_transaction(storage_with_lakehouse)
    namespace_name = "default"
    namespace_def = NamespaceDef()

    trinity_lake.create_namespace(storage_with_lakehouse, transaction, namespace_name, namespace_def)

    trinity_lake.drop_namespace(storage_with_lakehouse, transaction, namespace_name)

    with pytest.raises(ValueError):
        trinity_lake.describe_namespace(storage_with_lakehouse, transaction, namespace_name)

def test_create_and_describe_table(storage_with_lakehouse):
    transaction = trinity_lake.begin_transaction(storage_with_lakehouse)
    namespace_name = "default"
    namespace_def = NamespaceDef()
    trinity_lake.create_namespace(storage_with_lakehouse, transaction, namespace_name, namespace_def)
    table_name = "table_a"

    table_def = TableDef()
    trinity_lake.create_table(storage_with_lakehouse, transaction, namespace_name, table_name, table_def)
    described = trinity_lake.describe_table(storage_with_lakehouse, transaction, namespace_name, table_name)

    assert described == table_def

def test_commit_transaction(storage_with_lakehouse):
    transaction = trinity_lake.begin_transaction(storage_with_lakehouse)
    transaction.running_tree.insert("test", "value")
    trinity_lake.commit_transaction(storage_with_lakehouse, transaction)

    assert storage_with_lakehouse.read_file(LATEST_HINT_FILE) == b"1"
    assert storage_with_lakehouse.exists(file_paths.get_root_node_path(1))

def test_show_namespaces(storage_with_lakehouse):
    transaction = trinity_lake.begin_transaction(storage_with_lakehouse)
    namespace_def = NamespaceDef()

    trinity_lake.create_namespace(storage_with_lakehouse, transaction, "default", namespace_def)
    trinity_lake.create_namespace(storage_with_lakehouse, transaction, "dupe", namespace_def)
    namespaces = trinity_lake.show_namespaces(transaction)

    assert "default" in namespaces
    assert "dupe" in namespaces

def test_show_tables(storage_with_lakehouse):
    transaction = trinity_lake.begin_transaction(storage_with_lakehouse)

    namespace_def = NamespaceDef()
    namespace_name = "default"
    trinity_lake.create_namespace(storage_with_lakehouse, transaction, namespace_name, namespace_def)
    table_def = TableDef()

    trinity_lake.create_table(storage_with_lakehouse, transaction, namespace_name, "table_a", table_def)
    trinity_lake.create_table(storage_with_lakehouse, transaction, namespace_name, "table_b", table_def)

    tables = trinity_lake.show_tables(transaction, namespace_name)

    assert "table_a" in tables
    assert "table_b" in tables
