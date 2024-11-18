# Namespace

A namespace is a container in a LakeHouse to organize different objects.

## Object Definition Schema

***Schema ID: 1***

| Field Name                    | Protobuf Type       | Description                                        | Required? | Default       |
|-------------------------------|---------------------|----------------------------------------------------|-----------|---------------|
| name                          | String              | A user-friendly name of this namespace             | Yes       |               |
| properties                    | Map<String, String> | Free form user-defined key-value string properties | No        |               |




