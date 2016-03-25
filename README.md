# Tindex

Tindex is a generic index for N dimensions plus time. The motivation for this library
were indexing issues encountered in [Prometheus](https://prometheus.io).

The general problem is indexing documents along an arbitrary number of dimensions.
Documents must be retrievable by selecting them along any combination of these
dimensions and their values.
Addtionally, documents appear and disappear at arbitrary points in time. This time
dimension must also be selectable on querying.

This library provides high-level interfaces to implement an approach based on an
inverted index. An implementation of those interfaces is provided using
[BoltDB](https://github.com/boltdb/bolt).

## Work in progress

