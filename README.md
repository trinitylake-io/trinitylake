# TrinityLake

***An Open Lakehouse Format for Big Data Analytics, ML & AI***

![TrinityLake Logo](https://github.com/trinitylake-io/trinitylake/blob/main/docs/logo/blue-text-horizontal.png?raw=true)

## Introduction

The TrinityLake format defines the objects in a Lakehouse and provides a consistent and efficient way for 
accessing and manipulating these objects. It offers the following key features:
- **Multi-object multi-statement transactions** with standard SQL `BEGIN` and `COMMIT` semantics
- **Consistent time travel and snapshot export** across all objects in the Lakehouse
- **Storage only** as a Lakehouse solution that works exactly the same way locally, on premise and in the cloud
- **Compatibility with open table formats** like Apache Iceberg, supporting both standard SQL `MANAGED` and `EXTERNAL` as well as federation-based access patterns.
- **Compatibility with open catalog standards** like Apache Iceberg REST Catalog specification, serving as a highly scalable yet extremely lightweight backend implementation

For more details about the format specification, and how to get started and use it with various open engines such as Apache Spark, 
please visit [trinitylake.io](https://trinitylake.io).

## Building
TrinityLake is built using Gradle with Java 11, 17, 21, or 23.

* Build and run tests: `./gradlew build`
* Build without running tests: `./gradlew build -x test -x integrationTest`
* Fix code style and formatting: `./gradlew spotlessApply`

## Join Us

This project is still at early development stage. 
If you are interested in developing this project with us together,
we mainly use [Slack (click for invite link)](https://join.slack.com/t/trinitylake/shared_invite/zt-2uukxce7a-CzY3rR9q~3f0PRmm7mgEFw) 
for communication. We also use [GitHub Issues](https://github.com/trinitylake-io/trinitylake/issues) 
and [GitHub Discussions](https://github.com/trinitylake-io/trinitylake/discussions) for discussion purpose. 

## Project Website Development

The project website is built using the [mkdocs-material](https://pypi.org/project/mkdocs-material/) framework with a few other plugins.

### First time setup

```bash
python3 -m venv env
source env/bin/activate
pip install mkdocs-material
pip install mkdocs-awesome-pages-plugin
```

### Serve website

```bash
source env/bin/activate
mkdocs serve
```
