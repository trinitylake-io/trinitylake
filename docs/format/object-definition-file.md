# Object Definition File

The schema of an object in a Trinity LakeHouse is defined using [protobuf](https://protobuf.dev/).
The exact definition of each object is serialized into protobuf streams binary files, suffixed with `.binpb`.
These files are called **Object Definition Files (ODF)**.

The TrinityLake format currently provides the following object definition schemas in the LakeHouse:

- [LakeHouse](./lakehouse.md)
- [Namespace](./namespace.md)
- [Table](./table/overview.md)

