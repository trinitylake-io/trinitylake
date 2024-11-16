# LakeHouse

When user creates a Trinity LakeHouse, it must set the global configurations upfront.
An update to the configuration would entail a potentially expensive change of the TrinityLake tree.
For example, changing the maximum object size or file size would entail rewriting all the key names in the tree.

## Definition File Schema

| Field Name                 | Protobuf Type | Description                              | Required? | Default       |
|----------------------------|---------------|------------------------------------------|-----------|---------------|
| name                       | String        | A user-friendly name of this LakeHouse   | Yes       |               |
| major_format_version       | Integer       | The major version of the format          | No        | 0             |
| order                      | Integer       | The order of the B-epsilon tree          | No        | 128           |
| object_name_max_size_bytes | Long          | The maximum size of an object in bytes   | No        | 128           |
| file_name_max_size_bytes   | Long          | The maximum size of a file name in bytes | No        | 256           |
| node_file_max_size_bytes   | Long          | The maximum size of a node file in bytes | No        | 1048576 (1MB) |




