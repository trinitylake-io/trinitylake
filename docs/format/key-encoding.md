# Key Encoding

!!! note

    we use the literal `[space]` to represent the space character (hex value 20) in this document for clarity

## First Byte

The first byte of a key in a TrinityLake tree is used to differentiate user-facing objects in Lakehouse vs
any other system-internal object definitions such as [Lakehouse](#lakehouse-key).
User-facing object keys must start with a `[space]`,
and system-internal object keys must not start with a `[space]`.

## System Internal Keys

In general, system internal keys do not participate in the TrinityLake tree key sorting algorithm and always stay in 
the designated node.

### Lakehouse Definition Key

The [Lakehouse definition file](./lakehouse.md) pointer is stored with key `lakehouse_def` in the root node.

### Previous Root Node Key

The pointer to the previous root node is stored with key `previous_root` in the root node.

### Rollback Root Node Key

The pointer to the root node that was rolled back from, if the root node is created during a [Rollback](./transaction.md#rollback-committed-version)
It is stored with key `rollback_from_root` in the root node.

### Version Key

The version of the root node, stored with the key `version` in the root node.

### Creation Timestamp Key

The key `created_at_millis` writes the timestamp that a node is created.

## Object ID Key

The object ID key is a UTF-8 string that uniquely identifies the object and also allows sorting it in a 
lexicographical order that resembles the object hierarchy in a Lakehouse.

### Object Name

The object name has maximum size in bytes defined in [Lakehouse definition file](./lakehouse.md), 
with one configuration for each type of object.

The following UTF-8 characters are not permitted in an object name:
- any control characters (hex value 00 to 1F)
- the space character (hex value 20)
- the DEL character (hex value 7F)

### Encoded Object Name

When used in an object ID key, the object name is right-padded with space up to the maximum size 
(excluding the initial byte). For example, a namespace `default` under Lakehouse definition 
`namespace_name_max_size_bytes=8` will have an encoded object name`[space]default[space]`.

### Encoded Object Definition Schema ID

The schema of the [object definition](./object-definition-file.md) has a numeric ID, 
and is encoded to a base64 value of size 4 with padding.

### Encoded Object ID

The encoded object ID that is used as the object ID key is defined as the following:

- Namespace: encoded namespace name + encoded schema ID
- Table: encoded namespace name + encoded table name + encoded schema ID
