# Upsert

Upsert is a commonly used operation, especially in CDC streaming use case.
However, it is not clearly defined in the SQL standard.
In TrinityLake, we assign a specific semantics to performing upsert against a managed table.
The operation means inserting or overwriting the value of a row given its primary key.
The ANSI-SQL standard uses the term `MERGE` with a matching and non-matching condition to describe this operation,
and typically performs the operation in batch mode:

```sql
MERGE INTO my_table as t
    USING rows_to_upsert as s
    ON t.primary_key_col = s.primary_key_col
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
```

Because TrinityLake expects to support managed tables of multiple formats,
the TrinitLake format itself would define an upsert mechanism that works across the formats.

## Upsert with Watermark and Tombstone Keys

TrinityLake tables support primary-key based upsert operation through the definition of watermark and tombstone keys.

### Watermark Key

The value of a watermark key represents the freshness of the specific row.
The larger the watermark key value means the data is more fresh and should be used for read.
A watermark key can be of any primitive data type.
A null value of the watermark key is the smallest value compared to any non-null values.
A user can define multiple watermark key columns, which we call a **Composite Watermark Key**

### Tombstone Key

The value of a tombstone key represents if a specific row is marked as deleted.
TrinityLake supports the following ways of expressions:

- If the key is a boolean type, and the value is `true`.
- If the key is a string type (char or varchar), `tombstone_string_key_value` field of [Table definition](overview.md) is defined, 
   and the actual value is equal to one in definition.
- If the key is a primitive type, and the value is not `NULL`.

There can only be one column used as a tombstone key. A composite tombstone key is not supported.

### Writer Requirement

The writers will perform append-only operation of new rows for all write operations.
The watermark key should be populated with value to indicate how fresh the row is,
  unless it is certain that the row does not exist in the past and in that case the watermark key can be `NULL`.

For delete specifically, tombstone key should write:

- `true` if the key is a boolean
- value of `tombstone_string_key_value` if the key is a string type (char or varchar) and `tombstone_string_key_value` is defined
- any arbitrary value of the specific primitive type

If there are other columns in the row that are not nullable, 
the writer can write arbitrary value in those columns to meet that requirement.

### Reader Requirement

Reader should compare all the rows with the same primary key using the watermark key to derive
the latest value of the row.

If the latest row has a tombstone key matching the expected expression,
then the row should be considered as deleted.

For example, consider a table with primary key `order_id(varchar)`, watermark key `ts(timestamptz)`, and tombstone key `deleted(boolean)`.

- If there are 2 rows `(order_id=1, ts=100)` and `(order_id=1, ts=200)`, then during the merge read case,
`(order_id=1, ts=200)` will be the result returned by the reader and `(order_id=1, ts=100)` will be discarded.
- If there are 2 rows `(order_id=2, ts=100)` and `(order_id=2, ts=200, deleted=true)`, then during the merge read case,
  row with `(order_id=2)` will be considered as deleted.

This definition works for both managed, federated and external tables, as long as they provide the proper watermark and tombstone key definition.