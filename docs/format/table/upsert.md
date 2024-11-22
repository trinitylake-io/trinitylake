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

## Upsert with Primary and Watermark Keys

TrinityLake tables support primary-key based upsert operation through the definition of primary keys 
and watermark columns.

The writers will perform append-only operation of rows, and the watermark columns are used to provide a way to compare
the rows with the same primary key to determine which one to use.

For example, consider a table with primary key `order_id`, and watermark column `ts`.
If there are 2 rows `(order_id=1, ts=100)` and `(order_id=1, ts=200)`, then during the merge read case,
`(order_id=1, ts=200)` will be the result returned by the reader and `(order_id=1, ts=100)` will be discarded.

This definition works for both managed, federated and external tables, as long as they provide the proper definition.

## Upsert with Schema Evolution

TODO