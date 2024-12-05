# Apache Iceberg Table Format

Apache Iceberg is one of the supported formats of a TrinityLake table.

## Managed Iceberg Table

TrinityLake managed Iceberg tables should be created without any format properties in the [table definition](./overview.md#object-definition-schema).
The TrinityLake format determines what works the best for managing an Iceberg table within a Trinity Lakehouse.

For a managed table, TrinityLake maps the data type to Iceberg in the following way:

| TrinityLake Type | Iceberg Type                                         |
|------------------|------------------------------------------------------|
| boolean          | boolean                                              |
| int2             | integer                                              |
| int4             | integer                                              |
| int8             | long                                                 |
| decimal(p, s)    | decimal(p, s)                                        |
| float4           | float                                                |
| float8           | double                                               |
| char(n)          | string                                               |
| varchar(n)       | string                                               |
| date             | date                                                 |
| time(p)          | time if p=6, else long                               |
| timetz           | long                                                 |
| timestamp(p)     | timestamp if p=6, timestamp_ns if p=9, else long     |
| timestamptz(p)   | timestamptz if p=6, timestamptz_ns if p=9, else long |
| fixed(n)         | fixed(n)                                             |
| varbyte(n)       | binary                                               |
| struct           | struct                                               |
| map              | map                                                  |
| list             | list                                                 |

Currently only Apache Parquet file format is supported when using a Trinitylake managed Iceberg table.

## External Iceberg Table

To use an external Iceberg table in TrinityLake, you can configure the following [format properties](./overview.md#object-definition-schema):

| Property          | Description                                                                                          | Required? | Default |
|-------------------|------------------------------------------------------------------------------------------------------|-----------|---------|
| metadata_location | The location of the Iceberg metadata file                                                            | Yes       |         |
| schema_on_read    | If the table is schema on read. If true, a schema must be provided as a part of the table definition | No        | false   |

For an external table, TrinityLake surfaces the data type in Iceberg to TrinityLake in the following way:

| Iceberg Type           | TrinityLake Type |
|------------------------|------------------|
| boolean                | boolean          |
| integer                | int4             |
| long                   | int8             |
| decimal(p, s)          | decimal(p, s)    |
| float                  | float4           |
| double                 | float8           |
| string                 | varchar          |
| date                   | date             |
| time if p=6, else long | time(6)          |
| timestamp              | timestamp(6)     |
| timestamptz            | timestamptz(6)   |
| timestamp_ns           | timestamp(9)     |
| timestamptz_ns         | timestamptz(9)   |
| fixed(n)               | fixed(n)         |
| binary                 | varbyte          |
| struct                 | struct           |
| map                    | map              |
| list                   | list             |

## Federated Iceberg Table

To use a federated Iceberg table in TrinityLake, you need to configure Iceberg 
[catalog properties](https://iceberg.apache.org/docs/latest/configuration/?h=catalog+properties#catalog-properties)
inside the [format properties](./overview.md#object-definition-schema).
TrinityLake will use the catalog properties to initialize an Iceberg catalog to federate into the external system
to perform read and write.
The federated table's data types will be surfaced to TrinityLake in the same way as external tables.
