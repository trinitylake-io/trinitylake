# Location

## Root LakeHouse Location

A Trinity LakeHouse should be created at a location that we call **Root Location**.

Although TrinityLake in general does not depend on the directory concept in file system,
The root location is expected to behave like a directory where all files in the LakeHouse is stored in
locations that have the root location as the prefix.

To avoid user confusion, we will always treat the root location as ending with a `/` even when the user input does not.
For example, if the user defines the root location as `s3://my-bucket/my-lakehouse`,
it is treated as `s3://my-bucket/my-lakehouse/` when used.

The LakeHouse root location is not stored within the TrinityLake format itself.
It is expected that a user would specify the root location at runtime.

## Relative File Location

Any file location in a TrinityLake format must always be the relative location
against the root location of the LakeHouse.

For example, if the LakeHouse root location is at `s3://my-bucket/my-lakehouse`,
and a file is at location `s3://my-bucket/my-lakehouse/my-table-definition.binpb`,
then the location value stored in the TrinityLake format should be `my-table-definition.binpb`.

## File Name Size

All files stored in TrinityLake format must have a maximum file name size defined in the [LakeHouse definition file](./lakehouse.md).

## Optimized File Name

A file name in the TrinityLake format is designed for optimized performance in storage.
Given an **Original File Name**, the **Optimized File Name** in storage can be calculated as the following:

1. Calculate the MurMur3 hash of the file name in bytes.
2. Get the first 20 bytes and convert it to binary string representation and use it as the prefix. This maximizes the throughput in object storages like S3.
3. For the first, second and third group of 4 characters in the prefix, further separated with `/`. This maximizes the throughput in file systems like HDFS if a full directory listing at root location is necessary.
4. Concatenate the prefix before the original file name using the `-` character.

For example, an original file name `my-table-definition.binpb` will be transformed to `0101/0101/0101/10101100-my-table-definition.binpb`.

Note that not all the file names will be optimized in this way.
A few system-internal files such as the [root node file](./storage.md#root-node-file-name) will not be stored using this scheme.