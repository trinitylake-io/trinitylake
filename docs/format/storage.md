# Storage

The LakeHouse tree in general follows the [storage layout of N-way search tree map](tree/search-tree-map.md#storage-layout).
Each node file is in the [Apache Arrow IPC format](https://arrow.apache.org/docs/format/Columnar.html#format-ipc).

## Node File Schema

| ID | Name   | Arrow Type | Description                                                                                       | Required? | Default |
|----|--------|------------|---------------------------------------------------------------------------------------------------|-----------|---------|
| 1  | key    | String     | Name of the key, following [Key Specification](./key.md)                                          | no        |         |
| 2  | pvalue | String     | File location pointer to the value of the key, following [Location Specification](./location.md)  | no        |         |
| 3  | pnode  | String     | File location pointer to the value of the node, following [Location Specification](./location.md) | no        |         |

## System-Reserved Rows for Root Node

System reserved keys such as `!` will appear as the top rows in the file.
Such keys do not exist in non-root node, and do not participate in the tree storage algorithm.

## Node Pointers

To read the node pointers, the reader for the tree should skip the keys until it reaches the ones starting with `[space]`.
There are exactly `N` rows of the file are reserved for the `N` pointers of each node for a tree of order `N`.

The fist row in these `N` rows must have `key` and `pvalue` as `NULL` because the first pointer points to all
keys that are smaller than the key of the second row.

A node might not have all `N` child nodes yet. If there are `k <= N` child nodes,
There will be `N-k` rows with all column values as `NULL`s.

## Write Buffer

The write buffer rows start after the node pointer rows.
These rows must have a `key` that is not `NULL`, and the `pnode` is always `NULL`.
When the `pvalue` is `NULL`, it is a message to delete the `key`.
When the `pvalue` is not `NULL`, it is a message to set the current `pvalue` of the key in the tree to the new one in the write buffer.

## Node File Size

Each node is targeted for the same specific size, which is configurable in the LakeHouse definition.

The estimated size of the `N` rows should be `N * (object_name_size_bytes_max * 2 + file_name_size-Bytes_max)`, 
and this value must be less than the node file size.
This remaining size is used as the write buffer for each node.