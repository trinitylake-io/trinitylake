# TrinityLake&trade;

***An Open LakeHouse Format for Big Data Analytics, ML & AI***

The TrinityLake format defines the objects in a LakeHouse and provides a consistent and efficient way for 
accessing and manipulating these objects. It offers the following key features:
- **Multi-object multi-statement transactions** with standard SQL `BEGIN` and `COMMIT` semantics
- **Consistent time travel and snapshot export** across all objects in the LakeHouse
- **Storage only** as a LakeHouse solution without vendor lock-in
- **Compatibility with open table formats** like Apache Iceberg with standard SQL `MANAGED` and `EXTERNAL` semantics and federation support
- **Compatibility with open catalog standards** like Apache Iceberg REST Catalog specification

For more details about the format and how to get started and use it with various open engines such as Apache Spark, 
please visit [trinitylake.io](https://trinitylake.io).

## Project Website Development

The project website is built using the [mkdocs-material](https://pypi.org/project/mkdocs-material/) framework.
You can follow its guide to install the library and serve the website locally by 
running `mkdocs serve`.