# Streaming

Streaming into a managed table is fundamentally in conflict with managing and tracking transactions in a LakeHouse,
because transaction tracking provides a discrete history for every single operation happening in a LakeHouse,
whereas streaming applications continuously issue commits against the table in a high frequency
and creates a continuous sequence of changes to a table.

The stance that the TrinityLake format takes on streaming is that, tt is not practical, and also not really useful 
to model every single streaming update in every table as a transaction in a LakeHouse.
Instead, we provide the constructs for users to reconcile streaming changes to the LakeHouse transactions
in a way that would still provide good ACID semantics guarantee.

## Transaction Materialization

To reconcile streaming into a part of the LakeHouse transaction framework,
We define the concept of **transaction materialization**.

A continuous stream of data ingestion into a table would create the following diverging effects:

- At table level, the managed table could have new versions created as the streaming application continuously runs.
- At LakeHouse level, the streamed versions are invisible. When necessary, a user can explicitly
  materialize a transaction to reflect newly streamed data as an actual transaction history in the LakeHouse.

Such transaction materialization actions should only be necessary at a much lower cadence,
for examples once every hour, once every day or even less frequent, depending on the actual business use cases.

This is not the typical "checkpointing" action in streaming applications.
The main purpose of such transaction materialization is for creating an explicit point in time that can be used
for consistent rollback, time travel or snapshot export of the entire LakeHouse.

A separated write or DDL operation against a table with streaming will force a transaction materialization
against the table.

## Read Isolation Modes

There ar 2 modes that a reader can choose from when reading a table with a streaming application:

### Read Transacted

In this mode, the reader only reads whatever is presented based on the transactions in the LakeHouse.
The behavior is identical to [the one described in the Transaction Specification](../transaction.md#read-isolation).

The pro of this mode is that it strictly follows the multi-object multi-statement transaction semantics with ACID enforcement.
The con of this mode is that the data will be stale.
If a table chooses to do one transaction materialization per day, then the data will be stale for one day in the worst case.

### Read Latest

In this mode, the reader will try to find the latest version of the table at the transaction start time.
This version is decided whenever the first access to the table happens in a transaction,
and it could be a version that is not the recorded as the transacted version of the table tracked by the LakeHouse. 
After the initial access, the table remains frozen at that specific version within a transaction for subsequent reads.

To use Read Latest mode, the ANSI-SQL standard for transaction is extended in the following way in TrinityLake:

```sql
BEGIN WITH ns1.table1

SELECT * FROM ns1.table1
          
COMMIT
```
The tables that the transaction begins with will be accessed to determine its version before any further operation
is performed in order to ensure isolation guarantee.
For single-statement transaction, the access mode can also be modified through any runtime configuration mechanism, such as:

```sql
SET 'trinitylake.managed-table.access-mode' = 'READ_LATEST'
SELECT * FROM ns1.table1
```
