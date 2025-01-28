# ANSI Definitions

In this document, we define the anomaly phenomena and isolation levels that are 
available based on the ANSI-SQL standard and used by the TrinityLake format.

## Dirty Read

A dirty read is defined as the following:

1. Transaction T1 modifies a data item.
2. Another transaction T2 then reads that data item before T1 performs a COMMIT or ROLLBACK.
3. If T1 then performs a ROLLBACK, T2 has read a data item that was never committed and so never really existed.

For example:

| Transaction T1                      | Transaction T2                          |
|-------------------------------------|-----------------------------------------|
| BEGIN;                              |                                         |
| SELECT age FROM users WHERE id = 1; |                                         |
| -- returns 20                       |                                         |
|                                     | BEGIN;                                  |
|                                     | UPDATE users SET age = 21 WHERE id = 1; |
| SELECT age FROM users WHERE id = 1; |                                         |
| -- returns 21                       |                                         |
|                                     | ROLLBACK;                               |

## Non-Repeatable Read

Non-repeatable read, a.k.a. fuzzy read, is defined as the following:

1. Transaction T1 reads a data item.
2. Another transaction T2 then modifies or deletes that data item and commits.
3. If T1 then attempts to reread the data item, it receives a modified value or discovers that the data item has been deleted.

For example:

| Transaction T1                      | Transaction T2                          |
|-------------------------------------|-----------------------------------------|
| BEGIN;                              |                                         |
| SELECT age FROM users WHERE id = 1; |                                         |
| -- returns 20                       |                                         |
|                                     | BEGIN;                                  |
|                                     | UPDATE users SET age = 21 WHERE id = 1; |
|                                     | COMMIT;                                 |
| SELECT age FROM users WHERE id = 1; |                                         |
| -- returns 21                       |                                         |


## Phantom Read

Phantom read is defined as the following:

1. Transaction T1 reads a set of data items satisfying some search condition.
2. Transaction T2 then creates, modifies or deletes data items that satisfy T1’s search condition and commits.
3. If T1 then repeats its read with the same search condition, it gets a set of data items different from the first read.

For example:

| Transaction T1                             | Transaction T2                      |
|--------------------------------------------|-------------------------------------|
| BEGIN;                                     |                                     |
| SELECT count(*) FROM users WHERE age < 20; |                                     |
| -- returns 2                               |                                     |
|                                            | BEGIN;                              |
|                                            | INSERT INTO users (1001, 'Amy', 18) |
|                                            | COMMIT;                             |
| SELECT count(*) FROM users WHERE age < 20; |                                     |
| -- returns 3                               |                                     |

## ANSI Isolation Levels

In ANSI standard, there are 4 isolation levels are defined based on the 3 read phenomena:

| Isolation Level / Read Phenomenon | Dirty Read | Non-Repeatable Read | Phantom Read |
|-----------------------------------|------------|-------------------|--------------|
| READ UNCOMMITTED                  | Possible   | Possible          | Possible     |
| READ COMMITTED                    |            | Possible          | Possible     |
| REPEATABLE READ                   |            |                   | Possible     |
| SERIALIZABLE                      |            |                   |              |


It is pretty clear that the first 3 isolation levels directly map to the read phenomena:

- READ UNCOMMITTED level could see uncommitted data and can cause dirty read
- READ COMMITTED level does not see the dirty read phenomenon
- REPEATABLE READ level does not see the non-repeatable read phenomenon

!!! note

    In some commercial solutions, the REPEATABLE READ level guarantees more than the ANSI definition. 
    PostgreSQL guarantees both no non-repeated and phantom read. 
    Oracle REPEATABLE READ is actually SERIALIZABLE.
    In TrinityLake, we will stick with the ANSI definition.

Naturally, the next isolation level should be called something like “no-phantom read”, but it is not the case.
A system with SERIALIZABLE isolation emulates serial transaction execution for all committed transactions,
as if transactions had been executed one after another, serially, rather than concurrently.

SERIALIZABLE describes the ideal state of a database system,
where all concurrent transactions can act as if they are committed sequentially.
The implementation of SERIALIZABLE typically involves getting locks for all related rows and also query ranges,
which reduces the concurrency of query.