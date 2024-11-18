# B-Epsilon Tree

The key issue with a normal B-tree is that 

A B-epsilon tree is a B-tree with a write optimization where each tree node contains a **Write Buffer**.
The write buffer holds **Messages** about PutKey and DeleteKey operations.

For example, here is a B-epsilon tree with some messages in the write buffers of different nodes:



