# Storage Layout

The Lakehouse tree in general follows the storage layout of [N-way search tree map](tree/search-tree-map.md#storage-layout).
Each node file is in the [Apache Arrow IPC format](https://arrow.apache.org/docs/format/Columnar.html#format-ipc).

## Node File Schema

| ID | Name  | Arrow Type | Description                                          | Required? | Default |
|----|-------|------------|------------------------------------------------------|-----------|---------|
| 1  | key   | String     | Name of the key                                      | no        |         |
| 2  | value | String     | The value of the key                                 | no        |         |
| 3  | pnode | String     | File location pointer to the value of the child node | no        |         |
| 4  | txn   | String     | Transaction ID for [write buffer](./#write-buffer)   | no        |         |

## System-Internal Rows for Root Node

[System-internal keys](./key-encoding.md#system-internal-keys) will appear as the top rows in the file.
There is no specific ordering required for the system-internal rows.

## Node Pointers

To read the node pointers, the reader for the tree should skip the keys until it reaches the ones starting with `[space]`.
There are exactly `N` rows of the file are reserved for the `N` pointers of each node for a tree of order `N`.

The fist row in these `N` rows must have `key` and `value` as `NULL` because the first pointer points to all
keys that are smaller than the key of the second row.

A node might not have all `N` child nodes yet. If there are `k <= N` child nodes,
There will be `N-k` rows with all column values as `NULL`s.

## Write Buffer

The write buffer rows start after the node pointer rows.
These rows must have a `key` that is not `NULL`, and the `pnode` is always `NULL`.

- When the `value` is `NULL`, it is a message to delete the `key`.
- When the `value` is not `NULL`, it is a message to set the current `pvalue` of the key in the tree to the new one in the write buffer.

New changes are appended to the bottom of the write buffer.

## Node File Size

Each node is targeted for the same specific size, which is configurable in the [Lakehouse definition](./lakehouse.md).

The estimated size of the `N` rows should be:

```
N * (
  namespace_name_size_max_bytes + 
  table_name_size_max_bytes + 
  file_path_size_max_bytes +
  4 bytes for schema ID with padding 
)
```

and this value must be less than the node file size.
This remaining size is used as the write buffer for each node.

For users that would like to fine-tune the performance characteristics of a TrinityLake tree,
this formula can be used to readjust the node file size to achieve the desired epsilon value.
