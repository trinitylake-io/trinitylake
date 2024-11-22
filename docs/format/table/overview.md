# Overview

A table is a collection of related data organized in tabular format; consisting of columns and rows.

## Object Definition Schema

***Schema ID: 3***

| Field Name        | Protobuf Type       | Description                                                                                | Required? | Default |
|-------------------|---------------------|--------------------------------------------------------------------------------------------|-----------|---------|
| name              | string              | A user-friendly name of this table                                                         | Yes       |         |
| schema            | Schema              | Schema of the table, see [Table Schema](./table-schema.md)                                 | Yes       |         |
| distribution_keys | repeated uint32     | The list of column IDs that are distribution keys                                          | No        |         |
| sort_keys         | repeated uint32     | The list of column IDs that are sort keys                                                  | No        |         |
| primary_keys      | repeated uint32     | The list of column IDs that are primary keys                                               | No        |         |
| unique_keys       | repeated uint32     | The list of column IDs that are not primary key but are unique                             | No        |         |
| watermark_keys    | repeated uint32     | The list of columns IDs that are used as watermark columns, see [Upsert](./upsert.md)      | No        |         |
| table_type        | string              | Table type, see [Table Type](./table-type.md)                                              | No        | MANAGED |
| format_properties | map<string, string> | Free form format-specific key-value string properties, e.g. [Apache Iceberg](./iceberg.md) | No        |         |
| properties        | map<string, string> | Free form user-defined key-value string properties                                         | No        |         |

