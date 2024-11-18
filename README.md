# TrinityLake

***An Open LakeHouse Format for Big Data Analytics, ML & AI***

![TrinityLake Logo](https://github.com/jackye1995/trinitylake/blob/initial/docs/logo/blue-text-horizontal.png?raw=true)

## Introduction

The TrinityLake format defines the objects in a LakeHouse and provides a consistent and efficient way for 
accessing and manipulating these objects. It offers the following key features:
- **Multi-object multi-statement transactions** with standard SQL `BEGIN` and `COMMIT` semantics
- **Consistent time travel and snapshot export** across all objects in the LakeHouse
- **Storage only** as a LakeHouse solution that works exactly the same way locally, on premise and in the cloud
- **Compatibility with open table formats** like Apache Iceberg, supporting both standard SQL `MANAGED` and `EXTERNAL` as well as federation-based access patterns.
- **Compatibility with open catalog standards** like Apache Iceberg REST Catalog specification, serving as a highly scalable yet extremely lightweight backend implementation

For more details about the format specification, and how to get started and use it with various open engines such as Apache Spark, 
please visit [trinitylake.io](https://trinitylake.io).

## Project Website Development

The project website is built using the [mkdocs-material](https://pypi.org/project/mkdocs-material/) framework with a few other plugins.

### First time setup

```bash
python3 -m venv env
source env/bin/activate
pip install -r mkdocs-requirements.txt
```

### Serve website

```bash
source env/bin/activate
mkdocs serve
```
