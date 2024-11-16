# Commit

## Mutual Exclusion of File Creation

A storage supports mutual exclusion of file creation when only one writer wins if there are multiple writers
trying to create the same file.
This is the key feature that TrinityLake relies on for enforcing ACID for search trees.

This feature is widely available in most storage systems, for example:

- On Linux file system through [symlink](https://linux.die.net/man/2/open)
- On HDFS through [atomic rename](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/filesystem.html#boolean_rename.28Path_src.2C_Path_d.29)
- on Amazon S3 through [PutObject with IF-NONE-MATCH](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax)

## Copy-on-Write (CoW)

Modifying a TrinityLake tree means modifying the content of the existing node files and creating new node files.
This modification process is **copy-on-write (CoW)**,
because modifying each node will require reading the existing content of the node file,
and rewriting a completely new node file that contains potentially parts of the existing content plus the updated content.

## Atomic Commit

For every write, we perform a set of CoWs against the tree.
All our writes will write a new file in a new location, rather than overwriting the existing file.
The writes will always rewrite root node.
This commits the root node.


## Read Isolation