# Snapshot Isolation

## Definition

SNAPSHOT ISOLATION is an isolation level that also avoids [dirty read](./ansi-definitions#dirty-read), 
[non-repeatable read](./ansi-definitions#non-repeatable-read) and [phantom read](./ansi-definitions#phantom-read) like SERIALIZABLE.

SNAPSHOT ISOLATION allows the transactions occurring concurrently to see the same snapshot or copy of the system 
as it was at the beginning of the transactions, thus allowing a second transaction to make changes to the data that 
was to be read by another concurrent transaction. This other transaction would not observe the changes made by the 
second transaction and would continue working on the previous snapshot of the system.

However, unlike SERIALIZABLE, concurrent transactions might not be able to act as if they are committed sequentially.
When this happens, it is called a **Serialization Anomaly**

## Write Skew

At this isolation level, a serialization anomaly called **Write Skew** could occur.
Write skew is defined as the following:

1. Suppose transaction T1 reads x and y, which are consistent with constraint C.
2. Then transaction T2 reads x and y, writes x, and commits.
3. Then T1 writes y.
4. If there were a constraint between x and y, commit serialization might be violated.

A famous example is the black and white marble update. Consider a table of marbles:

| ID | Color |
|----|-------|
| 1  | Black |
| 2  | Black |
| 3  | White |
| 4  | White |

and 2 transactions can happen in the following order:

| Transaction T1                                            | Transaction T2                                            |
|-----------------------------------------------------------|-----------------------------------------------------------|
| BEGIN;                                                    |                                                           |
| UPDATE marbles set color = 'White' WHERE color = 'Black'; |                                                           |
|                                                           | BEGIN;                                                    |
|                                                           | UPDATE marbles set color = 'Black' WHERE color = 'White'; |
|                                                           | COMMIT;                                                   |
| COMMIT;                                                   |                                                           |

Under SERIALIZABLE, transaction T1 will either fail to commit, 
or commit all marbles to be white to present a serial commit history of T1 after T2.

However, under SNAPSHOT ISOLATION, both transactions would succeed without conflict, 
resulting in half marbles white and half black. 
This result cannot be produced by any serial execution order of T1 and T2, 
which means it is a serialization anomaly. 
But this does not trigger any read phenomenon including phantom read, 
because the search condition of `where color = 'Black'` does not overlap with `where color = 'White'`.