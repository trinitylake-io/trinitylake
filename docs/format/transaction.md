---
title: Transaction
---

# Transaction and ACID Enforcement

## Storage Requirements

### Mutual Exclusion of File Creation

A storage supports mutual exclusion of file creation when only one writer wins if there are multiple writers
trying to write to the same new file.
This is the key feature that TrinityLake relies on for enforcing ACID semantics during the commit process.

This feature is widely available in most storage systems, for examples:

- On Linux File System through [O_EXCL](https://linux.die.net/man/2/open)
- On Hadoop Distributed File System through [atomic rename](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/filesystem.html#boolean_rename.28Path_src.2C_Path_d.29)
- On Amazon S3 through [IF-NONE-MATCH](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax)
- On Google Cloud Storage through [IF-NONE-MATCH](https://cloud.google.com/storage/docs/xml-api/reference-headers#ifnonematch)
- On Azure Data Lake Storage through [IF-NONE-MATCH](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations)

### File Creation/Modification Timestamp

Storage systems typically provide metadata about when the file is created or modified.
In our use case, these 2 values are equivalent because as we see later in [Immutable Copy-on-Write](#immutable-copy-on-write-cow) 
the TrinityLake tree node files are immutable once written until deletion.
We do not require such timestamp to be exactly accurate, and it is only used for [time travel](#time-travel) purpose.

### Consistency

The **C**onsistency aspect of ACID is enforced in the storage system and out of the control of the TrinityLake format.
The TrinityLake format assumes that you are using a storage system that is **Strongly Consistent**, i.e.
all data operations are processed and reflected in a consistent order across a distributed system.
For example, the TrinityLake format would not work as expected if you use it on eventually consistent systems like Apache Cassandra.

### Durability

The **D**urability aspect of ACID is enforced in the storage system and out of the control of the TrinityLake format.
For example, if you use the TrinityLake format on Amazon S3, you get 99.999999999% (11 9's) durability guarantee.

## Immutable Copy-on-Write (CoW)

Modifying a TrinityLake tree means modifying the content of the existing [node files](./storage.md) and creating new node files.
This modification process is **Copy-on-Write (CoW)**, 
because "modifying the content" entails reading the existing content of the node file,
and rewriting a completely new node file that contains potentially parts of the existing content plus the updated content.

When performing CoW, the node files are required to be created at a new file location, rather than overwriting an existing file.
This means all node files are immutable once written until deletion.

## Root Node File Name

With CoW, the root node file name is important because every change to the tree would create a new root node file,
and the root node file name can be used essentially as the version of the tree.

TrinityLake defines that each root node has a numeric version number, 
and the root node is stored in a file name `_<version_number_binary_reversed>.ipc`.
The file name is persisted in storage as is without [optimization](./location.md#optimized-file-name).
For example, the 100th version of the root node file would be stored with name `_00100110000000000000000000000000.ipc`.

## Root Node Latest Version Hint File

A file with name `_latest_hint` is stored and marks the hint to the latest version of the TrinityLake tree root node file.
The file name is persisted in storage as is without [optimization](./location.md#optimized-file-name)
The file contains a number that marks the presumably latest version of the tree root node, such as `100`.

## Read Isolation

Here we discuss how the **I**solation aspect of ACID is enforced in TrinityLake.

A transaction, either for read or write or both, 
will always start with identifying the version of the TrinityLake tree to look into.
This is determined by:

1. Reading the version `_latest_hint` file if the file exists, or start from version 0
2. Try to get files of increasing version number until the version `k` that receives a file not found error
3. The version `k-1` will be the one to decide the [root node file name](#root-node-file-name)

All the object definition resolutions within the specific transaction must happen using that version of the TrinityLake tree.

## Commit Atomicity

Here we discuss how the **A**tomicity aspect of ACID is enforced in TrinityLake.

When committing a transaction, the writer does the following:

1. Apply changes and write all non-root node files
2. Try to write to the root node file in the targeted [root node file name](#root-node-file-name)
3. If succeeded, the commit has succeeded, write the `_latest_hint` file with the new version with best effort.
4. If failed, the commit has failed, and the process will decide the best way to re-apply the changes, which can be 
   either re-applying the metadata change against the new tree, or redo the entire operation, or anything in between
   depending on the implementation of the format.

!!! note

    At step 3, the write of the `_latest_hint` file is not guaranteed to exist or be accurate.
    For example, if two processes A and B commit sequentially at version 2 and 3, but A wrote the hint slower than B, 
    the hint file will be incorrect with value 2. This is why in the [Read Isolation](#read-isolation) section 
    we explicitly try to seek for further versions instead of just trust the value in the hint file.

## Time Travel

Because the TriniyLake tree node is versioned, time travel against the tree root, 
i.e. time travel against the entire Trinity LakeHouse, is possible.

The engines should match the time travel ANSI-SQL semantics in the following way:

- `FOR SYSTEM_VERSION AS OF`: the numeric version of the tree root,
- `FOR SYSTEM_TIME AS OF`: the [creation/modification time](#file-creationmodification-timestamp) 
of the tree root node file.

!!! note
    
    There is a fundamental difference between the TrinityLake time travel semantics versus the one in open table formats
    like Iceberg and Delta. TrinityLake leverages the system timestamp reported by the storage as the time travel basis.
    This is because we consider the competion of committing to the storage as the completion of transaction.
    Leveraging anything maintained within the format, e.g. a timestamp value in a metadata file, would not reflect
    the true transaction completion time in storage. Although the  

## Snapshot Export

A snapshot export for a Trinity LakeHouse means to export a specific version of the TrinityLake tree root node,
and all the files that are reachable through that root node.









