from const import PROTO_BINARY_FILE_SUFFIX, LAKEHOUSE_DEF_FILE_PREFIX, IPC_FILE_SUFFIX


def new_lakehouse_def_path() -> str:
    return f"{LAKEHOUSE_DEF_FILE_PREFIX}{_generate_uuid()}{PROTO_BINARY_FILE_SUFFIX}"


def new_namespace_def_path(namespace_name: str) -> str:
    return f"{LAKEHOUSE_DEF_FILE_PREFIX}{namespace_name}{PROTO_BINARY_FILE_SUFFIX}"


def get_root_node_path(version: int) -> str:
    return f"_{version:032b}{IPC_FILE_SUFFIX}"


def _generate_uuid():
    from uuid import uuid4

    return str(uuid4())
