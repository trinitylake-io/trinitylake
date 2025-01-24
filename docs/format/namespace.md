# Namespace

A namespace is a container in a Lakehouse to organize different objects.

## Object Definition Schema

***Schema ID: 1***

| Field Name                    | Protobuf Type       | Description                                        | Required? | Default       |
|-------------------------------|---------------------|----------------------------------------------------|-----------|---------------|
| name                          | string              | A user-friendly name of this namespace             | Yes       |               |
| properties                    | map<string, string> | Free form user-defined key-value string properties | No        |               |


## Name Size

All namespace names must obey the maximum size configuration defined in the [Lakehouse definition file](./lakehouse.md).

