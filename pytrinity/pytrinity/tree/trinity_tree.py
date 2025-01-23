from bisect import bisect_left


class TrinityNode:
    def __init__(self):
        self.system_rows = []
        self.buffer = []
        self.rows = []
        self.children = []

    def is_leaf(self):
        return len(self.children) == 0


class TrinityTree:
    def __init__(self, root_node: TrinityNode = None, degree=400, buffer_size=300):
        self.root = root_node
        self.degree = degree
        self.buffer_size = buffer_size

    def get(self, key: str):
        return self._search(self.root, key)

    def insert(self, key: str, value):
        self.root.buffer.append((key, value))

        if self._is_buffer_full(self.root):
            self._flush_buffer(self.root)

    def delete(self, key: str):
        try:
            self.get(key)
        except:
            raise KeyError(f"Key {key} not found")
        self.insert(key, None)

    def range_query(self, start: str, end: str):
        result = []
        self._range_query_helper(self.root, start, end, result)
        return result

    def _search(self, node: TrinityNode, key):
        # first check the buffer in order of most recent to oldest
        for k, v in reversed(node.buffer):
            if k == key:
                if v is None:
                    raise KeyError(f"Key {key} not found")
                return v

        # then check the keys in the node
        for k, v in node.rows:
            if k == key:
                return v

        if not node.is_leaf:
            # TODO: find correct child
            return self._search(node.children[0], key)
        raise KeyError(f"Key {key} not found")

    def _is_buffer_full(self, node: TrinityNode):
        return len(node.buffer) > self.buffer_size

    def _flush_buffer(self, node: TrinityNode):
        if node.is_leaf():
            self._merge_buffer_into_rows(node)
        else:
            for k, v in node.buffer:
                child_idx = bisect_left(node.children, k)
                node.children[child_idx].buffer.append((k, v))
            node.buffer.clear()

            for child in node.children:
                if self._is_buffer_full(child):
                    self._flush_buffer(child)

    def _merge_buffer_into_rows(self, node: TrinityNode):
        seen = {}
        # left to right because latest will overwrite
        for key, value in node.buffer:
            seen[key] = value

        for key, value in node.rows:
            if key not in seen:
                seen[key] = value
        node.rows = sorted([(k, v) for k, v in seen.items()])
        node.buffer.clear()
        return node

    def _split_node(self):
        pass

    def _range_query_helper(self, node: TrinityNode, start: str, end: str, result):
        for key, value in node.buffer:
            if start <= key <= end and value is not None:
                result.append((key, value))

        for key, value in node.rows:
            if start <= key <= end:
                result.append((key, value))

        if not node.is_leaf():
            start_idx = bisect_left([k for k, _ in node.rows], start)
            for child in node.children[start_idx:]:
                self._range_query_helper(child, start, end, result)
