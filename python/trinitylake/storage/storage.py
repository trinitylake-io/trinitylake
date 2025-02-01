import os
import time
import uuid
from typing import Optional
from urllib.parse import urlparse

from pyarrow import ipc, memory_map, array, string, OSFile, schema, record_batch, field

from const import CREATED_AT
from trinitylake.tree import TrinityNode


class Storage:
    NODE_SCHEMA = schema(
        [
            field("key", string(), nullable=True),
            field("value", string(), nullable=True),
            field("pnode", string(), nullable=True),
        ]
    )

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

    def serialize_protobuf(self, proto_msg, file_name=None) -> str:
        if not file_name:
            file_name = f"{str(uuid.uuid4())}.ipc"
        serialized_data = proto_msg.SerializeToString()
        file_path = self._full_path(file_name)
        with open(file_path, "wb") as f:
            f.write(serialized_data)
        return file_name

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
            pnodes.append("system_row")

        row_count = len(node.rows) + 1 if node.rows else 0
        for i in range(row_count):
            if i < len(node.rows):
                key, value = node.rows[i]
                keys.append(key)
                values.append(value)
            else:
                keys.append(None)
                values.append(None)
            if i < len(node.children):
                child = node.children[i]
                if child:
                    child_file_path = f"{str(uuid.uuid4())}.ipc"
                    self.serialize_tree(child, child_file_path)
                    pnodes.append(child_file_path)
                else:
                    pnodes.append(None)
            else:
                pnodes.append(None)

        for key, value in node.buffer:
            keys.append(key)
            values.append(value)
            pnodes.append("buffer_row")

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

        for i in range(row_count):
            if keys[i] and values[i] and pnodes[i] == "system_row":
                node.system_rows.append((keys[i], values[i]))
            elif keys[i] and values[i] and pnodes[i] == "buffer_row":
                node.buffer.append((keys[i], values[i]))
            else:
                key, value, child_node = None, None, None
                if i < len(keys):
                    key = keys[i]
                if i < len(values):
                    value = values[i]
                if i < len(pnodes) and pnodes[i]:
                    child_node = self._deserialize_node(pnodes[i])
                node.rows.append((key, value))
                node.children.append(child_node)
        return node

    def _read_ipc_file(self, path: str):
        with memory_map(path, "rb") as root_node:
            reader = ipc.RecordBatchFileReader(root_node)
            return reader.read_all()

    def _full_path(self, path: str) -> str:
        return os.path.join(self.root_path, path)

    def _generate_uuid(self):
        return str(uuid.uuid4())

    def write_node(self, node: TrinityNode, file_name: str):
        node.system_rows.append((CREATED_AT, str(time.time() * 1000)))
        self.serialize_tree(node, file_name)
