# Storage Location

In the [Storage Layout](./storage-layout.md) document, we have described how a TrinityLake tree is persisted in a storage.
This document describes the specification for the location of persisted files.

## Terminology

We in general use 3 terminologies:

- Path: a path 
- URI: a URI is a fully qualified address with scheme, authority, path components
- Location: a location can be either a relative path, or a fully qualified URI

## Root in Lakehouse Storage

A Trinity Lakehouse always starts at a storage **Root** location.
The root location must be a fully qualified URI, for example `s3://my-trinity-lakehouse/`.
All the 

## Directory in a storage

Although TrinityLake in general does not depend on the directory concept in file system,
The root location is expected to behave like a directory where all files in the Lakehouse are stored in
locations that have the root location as the prefix.

To avoid user confusion, we will always treat the root location as ending with a `/` even when the user input does not.
For example, if the user defines the root location as `s3://my-bucket/my-lakehouse`,
it is treated as `s3://my-bucket/my-lakehouse/` when used.

The Lakehouse root location is not stored within the TrinityLake format itself.
It is expected that a user would specify the root location at runtime.
This ensures the whole lakehouse is portable for use cases like replication.

## File Path in Lakehouse Storage

A **File Path** in a TrinityLake format should always be the relative location
against the root location of the Lakehouse.

For example, if the Lakehouse root location is at `s3://my-bucket/my-lakehouse`,
and a file is at location `s3://my-bucket/my-lakehouse/my-table-definition.binpb`,
then the location value stored in the TrinityLake format should be `my-table-definition.binpb`.

!!!Note

    Not all file locations follow this rule. Here are the exceptions:
    
    - [External table location](./table/table-type.md#external)


## Standard File Paths

### File Path Optimization

A file path in the TrinityLake format is designed for optimized performance in storage.
Given an **Original File Name**, the **Optimized File Name** in storage can be calculated as the following:

1. Calculate the MurMur3 hash of the file name in bytes.
2. Get the first 20 bytes and convert it to binary string representation and use it as the prefix. 
   This maximizes the throughput in object storages like S3.
3. For the first, second and third group of 4 characters in the prefix, further separated with `/`. 
   This maximizes the throughput in file systems when a directory listing at root location is necessary.
4. Concatenate the prefix before the original file name using the `-` character.

For example, an original file name `my-table-definition.binpb` will be transformed to 
`0101/0101/0101/10101100-my-table-definition.binpb`.

!!!Warning
    
    File name optimization is a write side feature, and should not be used by readers to reverse-engineer
    the original file name.


### Non-Root Node File Path

Non-root node file name will be in the form of prefix `node-` plus a version 4 UUID with suffix `.ipc`.
For example, if a UUID `6fcb514b-b878-4c9d-95b7-8dc3a7ce6fd8` is generated for the node file,
the original file name of the node file will be `node-6fcb514b-b878-4c9d-95b7-8dc3a7ce6fd8.ipc`,
and that further goes through the [file name optimization](./storage-location#optimized-file-name)
to produce the final node file name.

### Object Definition File Path

TODO


## Non-Standard File Paths

### Root Node File Path

With CoW, the root node file name is important because every change to the tree would create a new root node file,
and the root node file name can be used essentially as the version of the tree.

TrinityLake defines that each root node has a numeric version number,
and the root node is stored in a file name `_<version_number_binary_reversed>.ipc`.
The file name is persisted in storage as is without [optimization](./storage-location#optimized-file-name).
For example, the 100th version of the root node file would be stored with name `_00100110000000000000000000000000.ipc`.

### Root Node Latest Version Hint File Path

A file with name `_latest_hint.txt` is stored and marks the hint to the latest version of the TrinityLake tree root node file.
The file name is persisted in storage as is without [optimization](./storage-location#optimized-file-name)
The file contains a number that marks the presumably latest version of the tree root node, such as `100`.


### Lakehouse Definition File Path

should be `_lakehouse_def_` plus UUID plus `.binpb`