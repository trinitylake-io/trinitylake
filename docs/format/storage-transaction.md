---
title: Storage Transaction
---

# Storage Transaction and ACID Enforcement

In this document, we discuss how the TrinityLake integrates with a storage system to deliver transactional features.

## Storage Requirements

A storage used in TrinityLake must have the following properties:

### Mutual Exclusion of File Creation

A storage supports mutual exclusion of file creation when only one writer wins if there are multiple writers
trying to write to the same new file.
This is the key feature that TrinityLake relies on for enforcing ACID semantics during the commit process.

This feature is widely available in most storage systems, for examples:

- On Linux File System through [O_EXCL](https://linux.die.net/man/2/open)
- On Hadoop Distributed File System through [atomic rename](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/filesystem.html#boolean_rename.28Path_src.2C_Path_d.29)
- On Amazon S3 through [IF-NONE-MATCH](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax)
- On Amazon DynamoDB through [conditional PutItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html)
- On Google Cloud Storage through [IF-NONE-MATCH](https://cloud.google.com/storage/docs/xml-api/reference-headers#ifnonematch)
- On Azure Data Lake Storage through [IF-NONE-MATCH](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations)

### Consistency

The **C**onsistency aspect of ACID is enforced in the storage system and out of the control of the TrinityLake format.
The TrinityLake format assumes that you are using a storage system that is **Strongly Consistent**, i.e.
all data operations are processed and reflected in a consistent order across a distributed system.
For example, the TrinityLake format would not work as expected if you use it on eventually consistent systems like Apache Cassandra.

### Durability

The **D**urability aspect of ACID is enforced in the storage system and out of the control of the TrinityLake format.
For example, if you use the TrinityLake format on Amazon S3, you get 99.999999999% (11 9's) durability guarantee.

## Immutable Copy-on-Write (CoW)

Modifying a TrinityLake tree in storage means modifying the content of the existing [node files](./storage-layout) and creating new node files.
This modification process is **Copy-on-Write (CoW)**, 
because "modifying the content" entails reading the existing content of the node file,
and rewriting a completely new node file that contains potentially parts of the existing content plus the updated content.

When performing CoW, the node files are required to be created at a new file location, rather than overwriting an existing file.
This means all node files are immutable once written until deletion.

## Read Isolation

Here we discuss how the **I**solation aspect of ACID is enforced in TrinityLake, at the storage layer.

A transaction, either for read or write or both, will always start with identifying the version of the TrinityLake tree 
to look into. This is determined by:

1. Reading the [version hint file](./storage-location.md#root-node-latest-version-hint-file-path) if the file exists, or start from version 0
2. Try to get files of increasing version number until the version `k` that receives a file not found error
3. The version `k-1` will be the one to decide the [root node file name](#root-node-file-name)

All the object definition resolutions within the specific transaction must happen using that version of the TrinityLake tree.
This ensures at least all the transactions begin with a specific version.
For how to commit changes to the lakehouse while ensuring a specific isolation level, 
read [Lakehouse Transaction](./lakehouse-transaction) for more details.

## Commit Atomicity

Here we discuss how the **A**tomicity aspect of ACID is enforced in TrinityLake at storage layer.

When committing a transaction, the writer does the following:

1. Apply changes and write all non-root node files
2. Try to write to the root node file in the targeted [root node file name](#root-node-file-name)
3. If succeeded, the commit has succeeded, write the `_latest_hint` file with the new version with best effort.
4. If failed, the transaction commit step has failed at the storage layer. Depending o nthe overall lakehouse transaction
    level, it might be possible to rebase and retry the commit. Read [Lakehouse Transaction](./lakehouse-transaction) for more details.

!!! note

    At step 3, the write of the `_latest_hint` file is not guaranteed to exist or be accurate.
    For example, if two processes A and B commit sequentially at version 2 and 3, but A wrote the hint slower than B, 
    the hint file will be incorrect with value 2. This is why in the [Read Isolation](#read-isolation) section 
    we explicitly try to seek for further versions instead of just trust the value in the hint file.

## Time Travel

Because the TriniyLake tree node is versioned, time travel against the tree root, 
i.e. time travel against the entire Trinity Lakehouse, is possible.

The engines should match the time travel ANSI-SQL semantics in the following way:

### FOR SYSTEM_TIME AS OF

The timestamp-based time travel can be achieved by continuously tracing the [previous root node key](./key-encoding.md#previous-root-node-key) 
to older root nodes, and check the [creation timestamp key](./key-encoding.md#creation-timestamp-key) until the right root
node for time travel is found.

### FOR SYSTEM_VERSION AS OF

When the system version is a numeric value, it should map to the version of the tree root node.
The root node of the specific version can directly be found based on the [root node file name](#root-node-file-name).

When the system version is a string that does not resemble a numeric value, it should map to a possible [exported snapshot](#snapshot-export).

## Rollback Committed Version

TrinityLake uses the roll forward technique for rolling back any committed version.
If the current latest root node version is `v`, and a user would like to rollback to version `v-1`,
Rollback is performed by committing a new root node with version `v+1` which is most identical to the root node file `v-1`,
with the difference that the root node `v` should be recorded as the [rollback root node key](./key-encoding.md#rollback-root-node-key).

## Snapshot Export

A snapshot export for a Trinity Lakehouse means to export a specific version of the TrinityLake tree root node,
and all the files that are reachable through that root node.

Every time an export is created, the [Lakehouse definition](definitions/lakehouse.md) should be updated to record the name of the export
and the root node file that the export is at.

There are many types of export that can be achieved, because the export process can decide to stop replication
at any level of the tree and call it an export.
At one extreme, a process can replicate any reachable files starting at the root node. We call this a **Full Export**.
On the other side, a process can simply replicate the specific version of tree root node, 
and all other files reachable from the root node are not replicated. We call this a **Minimal Export**.
We call any export that is in between a **Partial Export**.

Any file that is referenced by both the exported snapshot and the source Lakehouse might be removed by the 
Lakehouse version expiration process.
With a full snapshot export, all files are replicated and dereferenced from the source Lakehouse.
With a partial or minimal export, additional retention policy settings are required to make sure the
version expiration process still keep those files available for a certain amount of time.
