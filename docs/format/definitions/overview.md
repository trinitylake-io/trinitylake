# Overview

## Schema

Each type of object definition has a different schema, which is defined using [protobuf](https://protobuf.dev/).
The schema should evolve in a way that is backward and froward compatible following the [versioning semantics](../versioning.md#versioning-semantics).

Each schema has a schema ID, and is used as a part of the TrinityLake tree [key encoding](../key-encoding.md#encoded-object-definition-schema-id).

The TrinityLake format currently provides the following object definitions with their corresponding schemas:

- [Lakehouse](lakehouse.md)
- [Namespace](namespace.md)
- [Table](table/overview.md)

## File Format

The exact definition of each object is serialized into protobuf streams binary files, suffixed with `.binpb`.
These files are called **Object Definition Files (ODF)**.
