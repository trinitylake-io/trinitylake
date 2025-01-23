from pytrinity.lakehouse import Lakehouse
from pytrinity.storage.storage import Storage
import tempfile

if __name__ == "__main__":
    path = tempfile.mkdtemp()
    print(f"Using path: {path}")
    storage = Storage(path)
    lh = Lakehouse(storage)

    txn = lh.begin_transaction()

    txn.create_namespace("default")

    ns = txn.describe_namespace("default")
    print(f"Created namespace: {ns.name}")

    lh.commit_transaction(txn)
    print("Committed transaction")

    txn2 = lh.begin_transaction()
    ns = txn.describe_namespace("default")
    print(f"Found namespace: {ns.name}")

    txn2.create_table("default", "test", None)

    tbl = txn2.describe_table("default", "test")

    print(f"Created table {tbl.name}")
    lh.commit_transaction(txn2)
