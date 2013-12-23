kcopy
=====

python script to copy between two cassandra clusters per node

This script is copying keys range owned by current node of source cluster
to destination cluster. So you can run this script on each source cluster node to archive
parallel copying.

Requirements: pycassa 1.10.0 or higher

Used by author to migrate from RandomPartitioner to Murmur3Partitioner.
Software is provided AS IS.

Usage:

1) Edit kcopy-rangeed.py and setup variables:

keyspace = 'KeyspaceName' # keyspace for source and destination cluster.
maxTTL = 2592000 # or None. Total seconds of data in source cluster that will be ignored if older than this value. Set None if you need copy without tt

maxRows = 10
maxColumnsPerRow = 4000 # maxRows * maxColumnsPerRow = columns number per one batch insert

columnFamily = 'ColumnFamilyName'

startToken = None # if transfer failed, you can start from last token. Type "grep process kcopy.log | tail" and copy start_token (the first) 

destinationCluster = ['192.168.1.1:9160','192.168.1.2:9160']

2) Run:
python kcopy-ranged.py <node ip address>


