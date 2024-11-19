# Table Overview

A table is a collection of related data organized in tabular format; consisting of columns and rows.

## Object Definition Schema

***Schema ID: 3***

| Field Name        | Protobuf Type       | Description                                                                                | Required? | Default |
|-------------------|---------------------|--------------------------------------------------------------------------------------------|-----------|---------|
| name              | String              | A user-friendly name of this table                                                         | Yes       |         |
| schema            | Schema              | Schema of the table, see [Table Schema](./table-schema.md)                                 | Yes       |         |
| distribution_keys | List<Integer>       | The list of column IDs that are distribution keys                                          | No        |         |
| sort_keys         | List<Integer>       | The list of column IDs that are sort keys                                                  | No        |         |
| primary_keys      | List<Integer>       | The list of column IDs that are primary keys                                               | No        |         |
| table_type        | String              | Table type, see [Table Type](./table-type.md)                                              | No        | MANAGED |
| format_properties | Map<String, String> | Free form format-specific key-value string properties, e.g. [Apache Iceberg](./iceberg.md) | No        |         |
| properties        | Map<String, String> | Free form user-defined key-value string properties                                         | No        |         |

