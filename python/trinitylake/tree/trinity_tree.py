from bisect import bisect_left, bisect_right
from collections import defaultdict
from typing import Optional, List


class TrinityNode:
    def __init__(
        self, parent: Optional["TrinityNode"] = None, is_leaf_node: bool = True
    ):
        self.system_rows = []
        self.buffer = []
        self.rows = []
        self.children = []
        self.parent = parent
        self.is_leaf_node = is_leaf_node

    def is_leaf(self):
        return self.is_leaf_node or len(self.children) == 0


class TrinityTree:
    def __init__(
        self, root_node: TrinityNode = None, order: int = 400, buffer_size: int = 300
    ):
        self.root = root_node
        self.order = order
        self.buffer_size = buffer_size

    def get(self, key: str):
        return self._search(self.root, key)

    def insert(self, key: str, value: Optional[str]):
        self.root.buffer.append((key, value))

        if self._should_flush_buffer(self.root):
            self._flush_buffer(self.root)

    def delete(self, key: str):
        try:
            self.get(key)
        except:
            raise KeyError(f"Key {key} not found")
        self.insert(key, None)  # tombstone

    def range_query(self, start: str, end: str):
        result = {}
        self._range_query_helper(self.root, start, end, result)
        return [(k, v) for k, v in result.items() if v]

    def _search(self, node: TrinityNode, search_key: str) -> Optional[str]:
        if not node:
            return

        # first check the buffer in order of most recent to oldest
        for key, val in reversed(node.buffer):
            if search_key == key:
                if val is None:
                    raise KeyError(f"Key {key} not found")
                return val

        # then check the keys in the node
        idx = bisect_left([k for k, _ in node.rows], search_key)
        if idx < len(node.rows) and node.rows[idx][0] == search_key:
            return node.rows[idx][1]

        # no more children to search
        if node.is_leaf():
            raise KeyError(f"Key {search_key} not found")

        child_idx = bisect_right([k for k, _ in node.rows], search_key)
        return self._search(node.children[child_idx], search_key)

    def _should_flush_buffer(self, node: TrinityNode):
        return len(node.buffer) > self.buffer_size

    def _flush_buffer(self, node: TrinityNode):
        if node.is_leaf():
            self._flush_leaf_node(node)
        else:
            self._flush_internal_node(node)

        if len(node.rows) > self.order:
            self._split_node(node)

    def _flush_leaf_node(self, node: TrinityNode) -> None:
        # first populate persisted rows
        merged_rows = {key: value for key, value in node.rows}

        # apply pending updates
        for key, value in node.buffer:
            if not value:
                merged_rows.pop(key)  # delete
            else:
                merged_rows[key] = value  # insert/update

        node.rows = sorted(merged_rows.items())
        node.children = [None] * (len(node.rows) + 1)
        node.buffer.clear()

    def _flush_internal_node(self, node):
        row_keys = [k for k, _ in node.rows]
        flush_map = defaultdict(list)

        # group messages by child nodes
        for key, value in node.buffer:
            child_idx = bisect_right(row_keys, key)
            flush_map[child_idx].append((key, value))

        # flush message batch to child nodes
        for idx, message in flush_map.items():
            child_node = node.children[idx]
            for key, value in message:
                child_node.buffer.append((key, value))
            if self._should_flush_buffer(child_node):
                self._flush_buffer(child_node)
        node.buffer.clear()

    def _split_node(self, node):
        split_idx = len(node.rows) // 2
        mid_key, mid_value = node.rows[split_idx]

        left = TrinityNode(parent=node)
        right = TrinityNode(parent=node)

        left.rows = node.rows[:split_idx]
        right.rows = node.rows[split_idx + 1 :]

        if not node.is_leaf():
            left.children = node.children[: split_idx + 1]
            right.children = node.children[split_idx + 1 :]

        else:
            left.children = [None] * (len(left.rows) + 1)
            right.children = [None] * (len(right.rows) + 1)

        if node == self.root:
            new_root = TrinityNode(is_leaf_node=False)
            new_root.rows = [(mid_key, mid_value)]
            new_root.children = [left, right]
            self.root = new_root
        else:
            # insert mid into parent
            parent = node.parent
            idx = bisect_left([k for k, _ in parent.rows], mid_key)
            parent.rows.insert(idx, (mid_key, mid_value))
            parent.children[idx] = left
            parent.children.insert(idx + 1, right)

    def _range_query_helper(self, node: TrinityNode, start: str, end: str, result):
        for key, value in reversed(node.buffer):
            if start <= key <= end and key not in result:
                result[key] = value

        for key, value in node.rows:
            if start <= key <= end and key not in result:
                result[key] = value

        if not node.is_leaf():
            start_idx = bisect_left([k for k, _ in node.rows], start)
            for child in node.children[start_idx:]:
                self._range_query_helper(child, start, end, result)
