kcopy
=====

python script to copy between two cassandra clusters per node

This script is copying keys range owned by current node of source cluster
to destination cluster. So you can run this script on each source cluster node to archive
parallel copying.

Requirements: datastax CQL python driver

Used by author to migrate from RandomPartitioner to Murmur3Partitioner.
Software is provided AS IS.

Usage:

1) Edit kcopy.py and setup:

keyspace for source and destination cluster.
select and insert statements
modify read_callback according to your data scheme

2) Run:
python kcopy.py

3) Read kcopy.log for errors

