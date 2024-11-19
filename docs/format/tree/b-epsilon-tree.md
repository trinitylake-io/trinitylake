# B-Epsilon Tree

As we see from the [process of updating a B-tree](./b-tree.md#example), 
The key issue with a normal B-tree is that the write amplification could be huge.
An update might change a large portion of nodes in the tree, which is not desirable for a system with a lot of writes.

## Background

A B-epsilon tree is a variant of B-tree that is more optimized for writes.
It was originally proposed in _[Lower Bounds for External Memory Dictionaries](http://perso.ens-lyon.fr/loris.marchal/docs-data-aware/brodal_fagerberg_LB_EM_dict.pdf)_ 
in 2003 as a way to demonstrate an asymptotic performance tradeoff curve between B-trees and buffered repository trees.
The concept is re-introduced for database applications in _[An Introduction to Bepsilon-trees and Write-Optimization](https://www.usenix.org/system/files/login/articles/login_oct15_05_bender.pdf)_
in 2015. Some applications of the B-epsilon tree include [BtrFS](https://btrfs.readthedocs.io/en/latest/), 
an implementation of the Linux file system using this data structure.

## Write

On top of the B-tree structure, B-epsilon tree introduces a **Write Buffer** in each node of the tree.
The write buffer holds **Messages** about the operations to be performed in the tree.
When a write happens, instead of updating a huge portion of the tree nodes, 
the writer simply writes a message in the message buffer.
If the message buffer is full, it sends the message down the node until a node where the buffer is not full,
and also applies the existing messages in the buffer to the nodes along the way if possible.

## Read

When reading, the reader starts from the root node and go down just like in B-tree.
However, in addition to walking the tree, it needs to apply any messages in the write buffers at runtime
to derive the latest value of a given key.
This is technically a **Merge-on-Read** for people that are familiar with that terminology. 

## Compaction

Because of the delayed write mechanism using write buffer, a compaction is possible against the tree,
where the process can force flush all the messages in the buffers to the corresponding keys to clear up the buffer space.
The process is not necessary because eventually the writes would bring down all the write buffers 
to the right nodes to be applied, but doing compaction wisely would improve the write performance further.
