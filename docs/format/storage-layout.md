# Storage Layout

The TrinityLake tree in general follows [the storage layout of N-way search tree map](./tree/search-tree-map.md).
In this document, we describe the details of the tree's layout in storage.

## Node File Format

Similar to a [N-way search tree map](./tree/search-tree-map.md), 
each node of the TrinityLake tree is a [node file](./tree/search-tree-map.md#node-file) in storage. 
Each file fully describes tabular data using the [Apache Arrow IPC format](https://arrow.apache.org/docs/format/Columnar.html#format-ipc).

## Node File Schema

The node file has the following schema:

| ID | Name  | Arrow Type | Description                                        | Required? | Default |
|----|-------|------------|----------------------------------------------------|-----------|---------|
| 1  | key   | String     | Name of the key                                    | no        |         |
| 2  | value | String     | The value of the key                               | no        |         |
| 3  | pnode | String     | Pointer to the path to the child node              | no        |         |
| 4  | txn   | String     | Transaction ID for [write buffer](./#write-buffer) | no        |         |

## Node File Content

Each node file contains 3 sections from top to bottom:

- System internal rows
- [Node key table](./tree/search-tree-map.md#node-key-table)
- Write buffer

They all share the same [node file schema](#node-file-schema) above, but use it in different ways.

## System-Internal Rows

Only the `key` and `value` columns in the [node file schema](#node-file-schema) are meaningful to system internal rows,
and they are required to be non-null.

These rows are used for recording system internal information such as node creation time, version, etc.
See [system-internal keys](./key-encoding.md#system-internal-keys) for more details.

There is no specific ordering expected for the system-internal rows, and there might be more system internal rows added over time.
because the first row of the [node key table](./tree/search-tree-map.md#node-key-table) must have `NULL` key and `NULL` value,
readers of a node file are expected to treat all rows before this row as system internal rows.

## Node Key Table

To read the node key table, the reader should skip all system internal rows, 
which means to skip all rows until it reaches the first row that has a `NULL` key and `NULL` value.

Then based on the rules of the [node key table](./tree/search-tree-map.md#node-key-table),
There are exactly `N` rows for the node key table section of the node file.

## Write Buffer

The [B-epsilon tree](./tree/b-epsilon-tree.md)-like write buffer of the TrinityLake tree starts after
the [system internal rows](#system-internal-rows) and the [node key table](#node-key-table) rows.

Each row in the write buffer represents a message to be applied to the TrinityLake tree.
New messages are appended at the bottom of the write buffer.
These rows have the following requirements:

1. `key` must not be `NULL`
2. `transaction` must not be `NULL`
3. `pnode` must be `NULL`
4. If `value` is `NULL`, it is a message to delete the key. If `value` is not `NULL`, it is a message to set the key to the specific value.

Note that different from a standard [B-epsilon tree](./tree/b-epsilon-tree.md),
when flushing write buffer against the TrinityLake tree during a [write](./tree/b-epsilon-tree.md#write) or 
[compaction](./tree/b-epsilon-tree.md#compaction), the messages in the latest committed transaction will not be flushed,
because it will be used for ensuring different level of isolation guarantee during Trinity Lakehouse commit phase.
See [Transaction and ACID Enforcement](./storage-transaction) for more details.

## Node File Size

Each node is targeted for the same specific size, which is configurable in the [Lakehouse definition](definitions/lakehouse.md).
Based on those configurations, users can roughly estimate the size of the node key table as:

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

