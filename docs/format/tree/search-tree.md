# Search Tree

A search tree is a tree data structure used for locating specific **Key**s from within a collection of keys,
and used as an implementation of a **Set**.

A search tree consists of a collection of **Node**s, and each node contains an ordered collection of keys.
For example, consider a search tree with integer keys:

![search-tree-example](search-tree-example.png)

To search if the key `64` is contained within the set:

1. Start from the top node `(1, 31)`, `64` is greater than `31` so go right
2. In `(58, 101)`, `64` is greater than `58` but smaller than `101`, so go down the pointer between `58` and `101`
3. In `(60, 64, 77)`, we find exact value for `64`
4. Conclusion reached that key `64` is contained within the set

## N-Way Search Tree

A N-way search tree is a search tree where:

1. A node with `k` children must have `k-1` number of keys.
2. Each node must have a maximum of `N` child nodes (i.e. each node must have a maximum of `N-1` keys)

The example above thus satisfies the requirement of being a 4-way search tree.
