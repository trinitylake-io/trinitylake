# Overview

A table is a collection of related data organized in tabular format; consisting of columns and rows.

## Object Definition Schema

***Schema ID: 3***

| Field Name                 | Protobuf Type       | Description                                                                                                      | Required? | Default |
|----------------------------|---------------------|------------------------------------------------------------------------------------------------------------------|-----------|---------|
| name                       | string              | A user-friendly name of this table                                                                               | Yes       |         |
| schema                     | Schema              | Schema of the table, see [Table Schema](./table-schema.md)                                                       | Yes       |         |
| distribution_keys          | repeated uint32     | The list of IDs for columns that are used as the distribution key                                                | No        |         |
| sort_keys                  | repeated uint32     | The list of IDs for columns that are used as sort key                                                            | No        |         |
| primary_key                | repeated uint32     | The list of IDs for columns that are used as primary key                                                         | No        |         |
| unique_columns             | repeated uint32     | The list of IDs for columns that are not used as primary key but are unique                                      | No        |         |
| watermark_key              | repeated uint32     | The list of IDs for columns that are used as watermark key, see [Upsert](./upsert.md)                            | No        |         |
| tombstone_key              | uint32              | The column ID that is used as the tombstone key, see [Upsert](./upsert.md)                                       |           |         |
| tombstone_string_key_value | string              | If tombstone key column is of type char or varchar, the string value that represents the delete marker           |           |         |
| table_type                 | string              | Table type, see [Table Type](./table-type.md)                                                                    | No        | MANAGED |
| table_format               | string              | The format of the table, which decides the usage of `format_properties`. Currently `ICEBERG` is the only option. | Yes       |         |
| format_properties          | map<string, string> | Free form format-specific key-value string properties, e.g. [Apache Iceberg](./iceberg.md)                       | No        |         |
| properties                 | map<string, string> | Free form user-defined key-value string properties                                                               | No        |         |

