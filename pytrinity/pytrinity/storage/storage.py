import os
import uuid
from typing import Optional
from urllib.parse import urlparse

from pyarrow import ipc, memory_map, array, string, OSFile, schema, record_batch, field
from ..tree.trinity_tree import TrinityNode


class Storage:
    NODE_SCHEMA = schema(
        [
            field("key", string(), nullable=True),
            field("value", string(), nullable=True),
            field("pnode", string(), nullable=True),
        ]
    )
    ROW_SEPERATOR = " "

    def __init__(self, directory):
        self.uri = urlparse(directory)
        self.root_path = self.uri.path
        os.makedirs(self.root_path, exist_ok=True)

    def get_root_path(self):
        return self.root_path

    def exists(self, path):
        return os.path.exists(self._full_path(path))

    def read_file(self, path):
        with open(self._full_path(path), "rb") as f:
            return f.read()

    def write_file(self, path, content):
        with open(self._full_path(path), "wb") as f:
            f.write(content)

    def serialize_protobuf(self, proto_msg) -> str:
        serialized_data = proto_msg.SerializeToString()
        uuid_str = f"{str(uuid.uuid4())}.binp"
        file_path = self._full_path(uuid_str)
        with open(file_path, "wb") as f:
            f.write(serialized_data)
        return uuid_str

    def deserialize_protobuf(self, file_name, proto_class) -> object:
        with open(self._full_path(file_name), "rb") as f:
            serialized_data = f.read()
        message = proto_class()
        message.ParseFromString(serialized_data)
        return message

    def serialize_tree(self, node: TrinityNode, node_file_name: str):
        keys = []
        values = []
        pnodes = []

        for key, value in node.system_rows:
            keys.append(key)
            values.append(value)
            pnodes.append(None)

        for key, value in node.rows:
            keys.append(key)
            values.append(value)
            pnodes.append(None)

        keys.append(self.ROW_SEPERATOR)
        values.append(self.ROW_SEPERATOR)
        pnodes.append(None)

        for child in node.children:
            child_file_path = f"{str(uuid.uuid4())}.ipc"
            self.serialize_tree(child, child_file_path)
            keys.append(None)
            values.append(None)
            pnodes.append(child_file_path)

        for key, value in node.buffer:
            keys.append(key)
            values.append(value)
            pnodes.append(None)

        batch = record_batch(
            [array(keys), array(values), array(pnodes)], schema=self.NODE_SCHEMA
        )

        full_path = self._full_path(node_file_name)
        with OSFile(full_path, "wb") as sink:
            with ipc.new_file(sink, self.NODE_SCHEMA) as writer:
                writer.write_batch(batch)

    def deserialize_tree(self, file_path: str) -> Optional[TrinityNode]:
        full_path = self._full_path(file_path)
        if not os.path.exists(full_path):
            return None
        root_node = self._deserialize_node(full_path)
        return root_node

    def _deserialize_node(self, file_path: str) -> TrinityNode:
        table = self._read_ipc_file(file_path)

        keys = table.column("key").to_pylist()
        values = table.column("value").to_pylist()
        pnodes = table.column("pnode").to_pylist()
        node = TrinityNode()
        row_count = len(keys)
        idx = 0

        while idx < row_count and keys[idx][0] != " ":
            node.system_rows.append((keys[idx], values[idx]))
            idx += 1

        while idx < row_count and keys[idx] and values[idx]:
            node.rows.append((keys[idx], values[idx]))
            idx += 1

        while idx < row_count and not keys[idx] and not values[idx]:
            node.children.append(self._deserialize_node(pnodes[idx]))
            idx += 1

        while idx < row_count and keys[idx] and values[idx]:
            node.buffer.append((keys[idx], values[idx]))
            idx += 1
        return node

    def _read_ipc_file(self, path: str):
        with memory_map(path, "rb") as root_node:
            reader = ipc.RecordBatchFileReader(root_node)
            return reader.read_all()

    def _full_path(self, path: str) -> str:
        return os.path.join(self.root_path, path)
