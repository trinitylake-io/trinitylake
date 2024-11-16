---
title: Welcome
---

# Welcome to TrinityLake

TrinityLake is an **Open LakeHouse Format** for Big Data Analytics, ML & AI. 
It defines the objects in a LakeHouse and provides a consistent and efficient way for accessing and manipulating these objects.
The TrinityLake format offers the following key features:

## Multi-Object Multi-Statement Transactions

TrinityLake enables multi-object multi-statement transactions across different tables, indexes, views, 
materialized views, etc. within a LakeHouse.
Users could start to leverage standard SQL BEGIN and COMMIT semantics and expect ACID enforcement 
at SNAPSHOT or SERIALIZABLE isolation level across the entire LakeHouse.

## Consistent Time Travel and Snapshot Export

TrinityLake provides a single timeline for all the transactions that have taken place within a LakeHouse.
Users can perform time travel to get a consistent view of all the objects in the LakeHouse,
and choose to export a snapshot of the entire LakeHouse at any given point of time.

## Storage Only

TrinityLake only leverages one storage primitive - mutual exclusion of file creation.
This means you can run TrinityLake on almost any storage solution including Linux file system, open source storage solutions like HDFS or MinIO, 
and cloud storage providers like Amazon S3, Google Cloud Storage, Azure Data Lake Storage.
You can build a truly open LakeHouse with TrinityLake without the need to pick a Catalog/DataLake/LakeHouse/Warehouse 
vendor and worry about potential vendor lock-in risks.

## Compatibility with Open Table Formats

TrinityLake can work with popular open table formats such as Apache Iceberg.
Users can create and use these tables with both the traditional SQL `MANAGED` or `EXTERNAL` experience,
as well as through federation when the table resides in other systems that can be connected to for read and write.

## Compatibility with Open Catalog Standards

TrinityLake can be used as an implementation of open catalog standards like the Apache Iceberg REST Catalog (IRC) specification.
The project provides an IRC server that user can run as a proxy to access TrinityLake and leverage all open source and 
vendor products that support IRC. This provides a highly scalable yet extremely lightweight IRC implementation 
where the server is mainly just an authorization engine, and the main execution logic is pushed down to the storage 
layer and handled by an open LakeHouse format.
