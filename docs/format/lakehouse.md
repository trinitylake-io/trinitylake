# LakeHouse

When user creates a Trinity LakeHouse, the LakeHouse definition must be determined upfront.
An update to the definition would entail a potentially expensive change of the TrinityLake tree.
For example, changing the maximum object size or file size would entail re-encode all the keys in the tree.

## Object Definition Schema

***Schema ID: 0***

| Field Name                    | Protobuf Type       | Description                                        | Required? | Default       |
|-------------------------------|---------------------|----------------------------------------------------|-----------|---------------|
| name                          | String              | A user-friendly name of this LakeHouse             | Yes       |               |
| major_format_version          | Integer             | The major version of the format                    | No        | 0             |
| order                         | Integer             | The order of the B-epsilon tree                    | No        | 128           |
| namespace_name_max_size_bytes | Long                | The maximum size of a namespace name in bytes      | No        | 100           |
| table_name_max_size_bytes     | Long                | The maximum size of a table name in bytes          | No        | 100           |
| file_name_max_size_bytes      | Long                | The maximum size of a file name in bytes           | No        | 200           |
| node_file_max_size_bytes      | Long                | The maximum size of a node file in bytes           | No        | 1048576 (1MB) |
| properties                    | Map<String, String> | Free form user-defined key-value string properties | No        |               |




