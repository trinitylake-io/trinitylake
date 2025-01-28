class ObjectKeyEncoder:
    # TODO: follow spec here for object key encoding
    @staticmethod
    def encode_name(name: str, max_size: int) -> str:
        encoded = name.encode("utf-8")
        if len(encoded) > max_size:
            raise ValueError(f"Name exceeds maximum size: {name}")
        return encoded.decode("utf-8")

    @staticmethod
    def encode_schema_id(schema_id: int) -> str:
        from base64 import b64encode

        schema_bytes = schema_id.to_bytes(4, byteorder="big")
        return b64encode(schema_bytes).decode("utf-8")
