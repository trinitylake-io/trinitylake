# B-Tree

An [N-way search tree](./search-tree.md#n-way-search-tree) only enforces the general requirements for the number of 
children per tree node. The tree could become imbalanced over time. A B-tree of order N is a self-balancing 
N-way search tree that enforces a set of rules when updating the tree:

1. All leaf nodes must appear at the same level
2. The root node must have at least 2 children, unless it is also a leaf
3. All nodes, except for the root node and leaves, must have at least `⌈N/2⌉` children

## Example

There are many tutorials online talking about B-Tree algorithms.
We will not describe all details here, but just demonstrate an example of how a B-tree is built from the bottom up.
Consider this B-tree of order 3 as the initial state:

![b-tree-example-initial](b-tree-example-initial.png)

Consider putting a new key `80` to the tree.

We start with going down the tree and putting the value to the correct leaf.

![b-tree-example-put-key-1](b-tree-example-put-key-1.png)

Because the leaf does not satisfy the N-way search tree requirement, 
we split the node and move the middle value to the parent node.

![b-tree-example-put-key-2](b-tree-example-put-key-2.png)

Now the parent node does not satisfy the N-way search tree requirement,
so we split the node and move the middle value to its parent node, which makes it a root node.

![b-tree-example-put-key-3](b-tree-example-put-key-3.png)

Now the tree satisfies the B-tree definition again, so the operation is completed.



