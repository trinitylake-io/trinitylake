---
title: Lakehouse Transaction
---

# Lakehouse Transaction and ACID Enforcement

In this document, we describe how ACID properties are enforced at Lakehouse level, leveraging
the [Storage Transaction](./storage-transaction.md) guarantees.

## Supported Isolation Levels

TrinityLake supports 2 isolation levels:

- [SNAPSHOT ISOLATION](./transaction/snapshot-isolation.md)
- [SERIALIZABLE](./transaction/ansi-definitions.md#ansi-isolation-levels)

This means out of the box, TrinityLake users do not need to worry about potential [dirty read](./transaction/ansi-definitions.md#dirty-read),
[non-repeatable read](./transaction/ansi-definitions.md#non-repeatable-read) and [phantom read](./transaction/ansi-definitions.md#phantom-read)
that could exist at lower isolation levels.

Depending on tolerance of serialization anomaly like [write skew](./transaction/snapshot-isolation.md#write-skew),
users can choose to set the isolation level to SNAPSHOT ISOLATION or SERIALIZABLE.
Using SERIALIZABLE would ensure fully serial transaction history, 
but reducing the transaction throughput of the whole Trinity lakehouse.

## Commit Conflict Resolution Strategy

When there is a concurrent transaction commit failure, the failing transaction that started at version `v` should do the following:

1. get the current latest version of the TrinityLake tree, denote this latest version as `w`
2. for each version between `v+1` and `w`, see if the changes can be applied while guaranteeing the isolation level of the transaction.
    1. if all the changes can be applied up to version `w`, redo the [storage commit process](./storage-transaction.md#storage-commit-process)
    2. if not, the transaction has failed at the lakehouse level.

### Example: concurrent commits to 2 different tables

Consider 2 transactions, T1 updating table `t1` and T2 updating table `t2`, both started at lakehouse version 3.
Assuming T1 commits successfully first, the following would happen for T2:

1. T2 commit against version 4 fails, due to version 4 root node file already exists
2. T2 checks the current latest version, which is 4
3. T2 sees in root node file of version 4 that the last transaction made change to `t1`
4. T2 determines that its change in `t2` does not conflict with `t1`
5. T2 now applies its change against version 4 root node file
6. T2 commits the new root node file against version 5
7. T2 commit succeeds

!!! note
   
    This conflcit resolution strategy is currently not fully correct. 
    For example, it is possible for update to `t2` to leverage infomration in `t1`, 
    causing the commit to be not serializable.
    We plan to introduce the concept of "Object Change Definition" to fix it, but in future iterations of the format. 
    