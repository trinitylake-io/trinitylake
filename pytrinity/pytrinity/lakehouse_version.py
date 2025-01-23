import time

from const import CREATED_AT
from pytrinity.const import (
    LAKEHOUSE_DEF,
    RESERVED_CHARS,
    SCHEMA_ID_TABLE,
    SCHEMA_ID_NAMESPACE,
    VERSION,
)
from pytrinity.object_key_encoder import ObjectKeyEncoder
from pytrinity.protobuf.Definitions_pb2 import NamespaceDef, TableDef, LakehouseDef
from pytrinity.storage import Storage
from tree.trinity_tree import TrinityNode, TrinityTree


class LakehouseVersion:
    SPACE = " "

    def __init__(self, version, storage: Storage, tree_path: str):
        self.version = version
        self.storage: Storage = storage
        self.lakehouse_def: LakehouseDef = None
        self.tree: TrinityTree = self._deserialize_tree(tree_path)

    def get_version(self):
        return self.version

    def describe_lakehouse(self):
        return self.lakehouse_def

    def describe_namespace(self, namespace_name) -> NamespaceDef:
        namespace_key = self.SPACE + ObjectKeyEncoder.encode_name(
            namespace_name, self.lakehouse_def.namespace_name_max_size_bytes
        )

        file_path = self.tree.get(namespace_key)
        return self.storage.deserialize_protobuf(file_path, NamespaceDef)

    def create_namespace(self, namespace_name, properties=None):
        self._validate_namespace(namespace_name)
        namespace_key = self.SPACE + ObjectKeyEncoder.encode_name(
            namespace_name, self.lakehouse_def.namespace_name_max_size_bytes
        )
        try:
            self.describe_namespace(namespace_name)
        except KeyError:
            pass

        namespace_def = NamespaceDef()
        namespace_def.name = namespace_name
        if properties:
            namespace_def.properties.update(properties)
        file_path = self.storage.serialize_protobuf(namespace_def)
        self.tree.insert(namespace_key, file_path)

    def show_namespaces(self):
        results = self.tree.range_query("", "\uffff")  # last possible item
        return [
            self.storage.deserialize_protobuf(r[1], NamespaceDef).name for r in results
        ]

    def drop_namespace(self, namespace_name):
        encoded_namespace = self.SPACE + ObjectKeyEncoder.encode_name(
            namespace_name, self.lakehouse_def.namespace_name_max_size_bytes()
        )
        if self.describe_namespace(namespace_name) is None:
            raise ValueError(f"Namespace {namespace_name} does not exist")
        self.tree.delete(encoded_namespace)

    def describe_table(self, namespace_name, table_name):
        encoded_namespace = self.SPACE + ObjectKeyEncoder.encode_name(
            namespace_name, self.lakehouse_def.namespace_name_max_size_bytes
        )
        encoded_table = ObjectKeyEncoder.encode_name(
            table_name, self.lakehouse_def.table_name_max_size_bytes
        )
        key = (
            encoded_namespace
            + encoded_table
            + ObjectKeyEncoder.encode_schema_id(SCHEMA_ID_TABLE)
        )
        file_path = self.tree.get(key)
        return self.storage.deserialize_protobuf(file_path, TableDef)

    def show_tables(self, namespace_name):
        encoded_namespace = self.SPACE + ObjectKeyEncoder.encode_name(
            namespace_name, self.lakehouse_def.namespace_name_max_size_bytes
        )
        if self.describe_namespace(namespace_name) is None:
            raise ValueError(f"Namespace {namespace_name} does not exist")

        results = self.tree.range_query(
            encoded_namespace + " ", encoded_namespace + "\uffff"  # last possible item
        )
        if results is None:
            return []

        return [self.storage.deserialize_protobuf(r[1], TableDef).name for r in results]

    def create_table(self, namespace_name, table_name, schema, properties=None):
        self._validate_namespace(namespace_name)
        self._validate_table(table_name)
        encoded_namespace = self.SPACE + ObjectKeyEncoder.encode_name(
            namespace_name, self.lakehouse_def.namespace_name_max_size_bytes
        )
        self.describe_namespace(namespace_name)

        table_def = TableDef()
        table_def.name = table_name
        if properties:
            table_def.properties.update(properties)

        encoded_table = ObjectKeyEncoder.encode_name(
            table_name, self.lakehouse_def.table_name_max_size_bytes
        )
        key = (
            encoded_namespace
            + encoded_table
            + ObjectKeyEncoder.encode_schema_id(SCHEMA_ID_TABLE)
        )
        file_path = self.storage.serialize_protobuf(table_def)
        self.tree.insert(key, file_path)

    def drop_table(self, namespace_name, table_name):
        encoded_namespace = self.SPACE + ObjectKeyEncoder.encode_name(
            namespace_name, self.lakehouse_def.namespace_name_max_size_bytes
        )
        encoded_table = ObjectKeyEncoder.encode_name(
            table_name, self.lakehouse_def.table_name_max_size_bytes
        )
        key = (
            encoded_namespace
            + encoded_table
            + ObjectKeyEncoder.encode_schema_id(SCHEMA_ID_TABLE)
        )

        if self.describe_namespace(namespace_name) is None:
            raise ValueError(f"Namespace {namespace_name} does not exist")

        if self.describe_table(namespace_name, table_name) is None:
            raise ValueError(f"Table {table_name} does not exist")

        self.tree.delete(key)

    def _validate_namespace(self, name: str):
        if any(c in RESERVED_CHARS for c in name):
            raise ValueError(f"Invalid characters in name: {name}")

        if len(name.encode("utf-8")) > self.lakehouse_def.namespace_name_max_size_bytes:
            raise ValueError(f"Name exceeds maximum size: {name}")

    def _validate_table(self, name: str):
        if len(name.encode("utf-8")) > self.lakehouse_def.table_name_max_size_bytes:
            raise ValueError(f"Name exceeds maximum size: {name}")

    def _deserialize_tree(self, tree_path: str) -> TrinityTree:
        if not self.storage.exists(tree_path):
            print("Initializing new tree")
            new_root = self._initialize_new_tree()
            return TrinityTree(new_root)
        else:
            print("Loading existing tree")
        root_node = self.storage.deserialize_tree(tree_path)
        for key, value in root_node.system_rows:
            if key == LAKEHOUSE_DEF:
                self.lakehouse_def = self.storage.deserialize_protobuf(
                    value, LakehouseDef
                )
        return TrinityTree(root_node)

    def _initialize_new_tree(self):
        root_node = TrinityNode()

        lakehouse_def = LakehouseDef()
        lakehouse_def.name = "default"
        lakehouse_def.major_version = 0
        lakehouse_def.order = 128
        lakehouse_def.namespace_name_max_size_bytes = 100
        lakehouse_def.table_name_max_size_bytes = 100
        lakehouse_def.file_name_max_size_bytes = 200
        lakehouse_def.node_file_max_size_bytes = 1048576
        lakehouse_def.minimum_versions_to_keep = 3
        lakehouse_def.maximum_version_age_millis = 604800000  # 7days
        lakehouse_def_path = self.storage.serialize_protobuf(lakehouse_def)

        self.lakehouse_def = lakehouse_def

        root_node.system_rows.append((LAKEHOUSE_DEF, lakehouse_def_path))
        root_node.system_rows.append((VERSION, "0"))
        root_node.system_rows.append((CREATED_AT, str(time.time() * 1000)))

        return root_node
