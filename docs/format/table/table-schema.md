# Schema

## Primitive Types

TrinityLake tables support the following primitive data types:

| Type           | Description                                                 | Aliases                            |
|----------------|-------------------------------------------------------------|------------------------------------|
| boolean        | True or false                                               | bool                               |
| int2           | Signed 2-byte integer                                       | smallint                           |
| int4           | Signed 4-byte integer                                       | int, integer                       |
| int8           | Signed 8-byte integer                                       | bigint, long                       |
| decimal(p, s)  | Exact numeric of selectable precision (p) and scale (s)     | numeric(p, s)                      |
| float4         | Single precision floating-point number                      | real                               |
| float8         | Double precision flaoting-point number                      | float8, float, double              |
| char(n)        | Fixed length character string of size n                     | character, nchar, bpchar           |
| varchar(n)     | variable length character string of optional maximum size n | character varying, nvarchar, text  |
| date           | Calendar date of year, month, day                           |                                    |
| time(p)        | time of a day with precision p                              | time without time zone             |
| timetz(p)      | time of a day with specific time zone with precision p      | time with time zone                |
| timestamp(p)   | Date and time on a wall clock with precision p              | timestamp without time zone        |
| timestamptz(p) | Date and time with a time zone and with precision p         | timestamp with time zone           |
| fixed(n)       | Fixed length binary value of size n                         |                                    |
| binary(n)      | Variable length binary value of optional maximum size n     | varbinary, binary varying, varbyte |

## Nested Types

TrinityLake tables support the following nested data types:

| Type   | Description                                                      | Aliases |
|--------|------------------------------------------------------------------|---------|
| struct | A tuple of typed values                                          | row     |
| map    | A collection of key-value pairs with a key type and a value type |         |
| list   | A collection of values with some element type                    | array   |
