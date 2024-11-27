# LakeHouse

LakeHouse is the top level container.

## Object Definition Schema

***Schema ID: 0***

| Field Name                           | Protobuf Type       | Description                                                                                                    | Required? | Default       |
|--------------------------------------|---------------------|----------------------------------------------------------------------------------------------------------------|-----------|---------------|
| name                                 | string              | A user-friendly name of this LakeHouse                                                                         | Yes       |               |
| major_version                        | uint32              | The major version of the format                                                                                | No        | 0             |
| order                                | uint32              | The order of the B-epsilon tree                                                                                | No        | 128           |
| namespace_name_max_size_bytes        | uint32              | The maximum size of a namespace name in bytes                                                                  | No        | 100           |
| table_name_max_size_bytes            | uint32              | The maximum size of a table name in bytes                                                                      | No        | 100           |
| file_name_max_size_bytes             | uint32              | The maximum size of a file name in bytes                                                                       | No        | 200           |
| node_file_max_size_bytes             | uint64              | The maximum size of a node file in bytes                                                                       | No        | 1048576 (1MB) |
| properties                           | map<string, string> | Free form user-defined key-value string properties                                                             | No        |               |
| maximum_version_age_millis           | uint64              | Maximum age of a version before expiration                                                                     | No        | 7 days        |
| minimum_versions_to_keep             | uint32              | The minimum number of versions to keep                                                                         | No        | 3             |
| maximum_version_age_millis_overrides | map<uint64, uint64> | The mapping of versions to their maximum age before expiration, if different from `maximum_version_age_millis` | No        |               |
| exported_snapshots                   | map<string, string> | The mapping of snapshot export name and corresponding root node file location                                  | No        |               |

!!!Note

    An update to some of the fields would entail a potentially expensive change of the TrinityLake tree.
    For example, changing the maximum object size or file size would entail re-encode all the keys in the tree.
