import pytest
from trinitylake.tree import TrinityTree, TrinityNode


@pytest.fixture
def empty_tree():
    return TrinityTree(root_node=TrinityNode(), order=3, buffer_size=2)


def test_basic_get_with_insert_and_update(empty_tree):
    empty_tree.insert("a", "a")
    empty_tree.insert("b", "b")

    assert empty_tree.get("a") == "a"
    assert empty_tree.get("b") == "b"

    # Test update
    empty_tree.insert("a", "a-updated")
    assert empty_tree.get("a") == "a-updated"


def test_basic_delete(empty_tree):
    empty_tree.insert("a", "a")
    empty_tree.delete("a")

    with pytest.raises(KeyError):
        empty_tree.get("a")


def test_range_query(empty_tree):
    data = ["a", "b", "c", "d"]
    for char in data:
        empty_tree.insert(char, char)

    results = empty_tree.range_query("a", "c")
    assert set(results) == {("a", "a"), ("b", "b"), ("c", "c")}

    empty_tree.delete("b")
    results = empty_tree.range_query("a", "c")
    assert set(results) == {("a", "a"), ("c", "c")}


def test_buffer_flushing(empty_tree):
    empty_tree.insert("a", "1")
    empty_tree.insert("b", "2")
    assert len(empty_tree.root.buffer) == 2

    empty_tree.insert("c", "3")
    assert len(empty_tree.root.buffer) == 0
    assert len(empty_tree.root.rows) == 3


def test_large_insertion(empty_tree):
    for i in range(100):
        empty_tree.insert(str(i), str(i))

    for i in range(100):
        assert empty_tree.get(str(i)) == str(i)
