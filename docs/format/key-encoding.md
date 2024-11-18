# Key Encoding

## First Byte

The first byte of a key in a TrinityLake tree is used as an indicator to differentiate user-facing objects in LakeHouse vs
any other system-reserved objects such as [LakeHouse](./lakehouse.md).
User-facing object keys must start with a `[space]`,
and system-reserved object keys must not start with a `[space]`.

!!! note

    we use the literal `[space]` to represent the space character (hex value 20) in this document for clarity

## LakeHouse Definition Key

The LakeHouse definition file pointer is stored with key `lakehouse`.
This key is special that it does not participate in the TrinityLake tree sorting algorithm,
and always stay in the root node and is read first by any reader.

## Object Name Key Encoding

The object name key is a UTF-8 string that contains multiple parts.

### Object Name

Any object name must have maximum size in bytes defined in Lakehouse definition.
The value must only contain UTF-8 characters that are not any control characters or the space character (hex value 00 to 20).

### Encoded Object Name

When encoded in an object key, the object name is right-padded with space up to the maximum size.
For example, a namespace `default` under LakeHouse definition `object_name_size_bytes_max=16` will have an encoded object name
`[space]default[space][space][space][space][space][space][space][space][space]`.

### Encoded Object ID

The full key name of an object 



The name of an object must have maximum size in bytes defined by global configuration `<object>.name_size_bytes_max`,
default to 128. The value must only contain UTF-8 string that is not any control characters or the space character (hex value 00 to 20).

When presented in an object key, the value is left-padded with space up to the maximum size of the namespace name.
For example, a namespace `default` under configuration `namespace.name_size_bytes_max=16` will have an object name key
`[space][space][space][space][space][space][space][space][space][space]default` (1 space for first byte, 9 spaces for padding, 7 bytes for `default`).
