LAKEHOUSE_DEF = "lakehouse_def"

PREVIOUS_ROOT = "previous_root"

ROLLBACK_ROOT = "rollback_from_root"

VERSION = "version"

CREATED_AT = "created_at_millis"

RESERVED_CHARS = set(chr(i) for i in range(0x00, 0x20)) | {" ", chr(0x7F)}

SCHEMA_ID_LAKEHOUSE = 0

SCHEMA_ID_NAMESPACE = 1

SCHEMA_ID_TABLE = 2

LATEST_HINT_FILE = "_latest_hint.txt"

PROTO_BINARY_FILE_SUFFIX = ".binpb"

LAKEHOUSE_DEF_FILE_PREFIX = "_lakehouse_def_"

IPC_FILE_SUFFIX = ".arrow"

SCHEMA_ID_PART_SIZE = 4

NAMESPACE_SCHEMA_ID_PART = "B==="

TABLE_SCHEMA_ID_PART = "C==="

HIGHEST_CHARACTER_VALUE = "\uffff"
