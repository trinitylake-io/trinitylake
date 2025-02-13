# Storage Location

In the [Storage Layout](./storage-layout.md) document, we have described how a TrinityLake tree is persisted in a storage.
This document describes the specification for the location of persisted files.

## Terminologies

### Directory vs Prefix Notation

In general, TrinityLake is designed against object storage systems, and only have a prefix concept.
There is no operation at TrinityLake interface level to create or manage directories in any storage systems that supports directory.

However, we retain the conventional directory concept and syntax, and users can say a prefix, or a part of a prefix, 
is a directory if the purpose of that prefix is to store more files, with `/` being the separator for different levels of directories.

### Types of Locations

We in general use 3 terminologies when describing the storage locations in TrinityLake:

- **URI**: a URI in TrinityLake follows the [RFC-3986](https://datatracker.ietf.org/doc/html/rfc3986) specification,
  but in addition obeys the object storage semantics, where special characters like `.`, `..`, etc. should be interpreted literally,
  rather than having semantic file system meanings. To check if a URI is qualified to be used in TrinityLake, evaluate the URI in a
  POSIX file system, and the resulting URI should be identical to the original URI. For example, `s3://my-bucket/../file` is evaluated
  to `s3://my-file`, and is thus not qualified and must not be used in TrinityLake.
- **Path**: when used in TrinityLake, a path refers to a sequence of directories, optionally with a final file name. 
  Therefore, a path must be relative to a prefix, and not start with `/`.
- **Location**: when used in TrinityLake, a location can be either a path, or a fully qualified URI

## Root URI

A Trinity Lakehouse always starts at a storage **Root** URI, for example `s3://my-trinity-lakehouse/`.
We call a storage system with a root URI a **Trinity Lakehouse Storage**, as all the internal information of this lakehouse
must be using paths within this root URI.

The root URI is always a directory, thus to avoid user confusion, we will always treat it as 
ending with a `/` even when the user input does not.
For example, if the user defines the root URI as `s3://my-bucket/my-lakehouse`,
it is treated as `s3://my-bucket/my-lakehouse/` when used.

The Lakehouse root URI is not stored within the TrinityLake format itself.
It is expected that a user would specify the root location at runtime.
This ensures the whole lakehouse is portable for use cases like cross-region replication.

## Standard File Paths

### File Path Optimization

A file path in the TrinityLake format is designed for optimized performance in storage, with a focus on object storage.
Given an **Original File Path**, the **Optimized File Path** in storage can be calculated as the following:

1. Calculate the MurMur3 hash of the file path in bytes.
2. Get the first 20 bytes and convert it to binary string representation and use it as the prefix. 
   This maximizes the throughput in object storages like S3.
3. For the first, second and third group of 4 characters in the prefix, further separated with `/`. 
   This maximizes the throughput in file systems when a directory listing at root location is necessary.
4. Replace the `/` character in original file path by `-`
5. Concatenate the prefix before the file path produced in step 4 using the `-` character.

For example, an original file path `my/path/my-table-definition.binpb` will be transformed to 
`0101/0101/0101/10101100-my-path-my-table-definition.binpb`.

!!!Warning
    
    File name optimization is a write side feature, and should not be used by readers to reverse-engineer
    the original file name.


### Non-Root Node File Path

Non-root node file paths are in the form of prefix `node-` plus a version 4 UUID with suffix `.arrow`.
For example, if a UUID `6fcb514b-b878-4c9d-95b7-8dc3a7ce6fd8` is generated for the node file,
the original file name of the node file will be `node-6fcb514b-b878-4c9d-95b7-8dc3a7ce6fd8.arrow`,
and that further goes through the [file name optimization](./storage-location#optimized-file-name)
to produce the final node file path.

### Object Definition File Path

Object definition file paths excluding the [Lakehouse definition file path](#lakehouse-definition-file-path)
are in the form of prefix `{object-type}-{object-identifier}-` plus a version 4 UUID
with suffix `.binpb`.
For example, a table `t1` in namespace `ns1` and UUID `6fcb514b-b878-4c9d-95b7-8dc3a7ce6fd8` would give a path
`table-t1-ns1-6fcb514b-b878-4c9d-95b7-8dc3a7ce6fd8.binpb`,
and that further goes through the [file name optimization](./storage-location#optimized-file-name)
to produce the final object definition file path.

## Non-Standard File Paths

### Root Node File Path

With CoW, the root node file name is important because every change to the tree would create a new root node file,
and the root node file name can be used essentially as the version of the tree.

TrinityLake defines that each root node has a numeric version number,
and the root node is stored in a file name `_<version_number_binary_reversed>.arrow`.
The file name is persisted in storage as is without [optimization](./storage-location#optimized-file-name).
For example, the 100th version of the root node file would be stored with name `_00100110000000000000000000000000.arrow`.

### Root Node Latest Version Hint File Path

A file with name `_latest_hint.txt` is stored and marks the hint to the latest version of the TrinityLake tree root node file.
The file name is persisted in storage as is without [optimization](./storage-location#optimized-file-name)
The file contains a number that marks the presumably latest version of the tree root node, such as `100`.

### Lakehouse Definition File Path

Lakehouse definition file path are in the form of `_lakehouse_def_` plus a version 4 UUID with suffix `.binpb`.
For example, a UUID `6fcb514b-b878-4c9d-95b7-8dc3a7ce6fd8` would give a path
`_lakehouse_def_6fcb514b-b878-4c9d-95b7-8dc3a7ce6fd8.binpb`.