#!/usr/bin/python

import sys, time, logging
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement, BatchType
from cassandra import ConsistencyLevel

keyspace = 'Your_Keyspace'

source = Cluster(
    ['192.168.52.1', '192.168.52.2'],
    port=9042)

dest = Cluster(
    ['192.168.58.3', '192.168.58.4'],
    port=9042)

FORMAT = "%(levelname)s %(asctime)-15s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG, filename='kcopy.log')

src_session = source.connect(keyspace=keyspace)
dst_session = dest.connect(keyspace=keyspace)

select = SimpleStatement('SELECT key, column1, value, TTL(value) FROM your_column_family', fetch_size=500)
select.consistency_level = ConsistencyLevel.QUORUM
insert = dst_session.prepare('INSERT INTO your_column_family (key, column1, value) VALUES (?, ?, ?)')
insert_ttl = dst_session.prepare('INSERT INTO your_column_family (key, column1, value) VALUES (?, ?, ?) USING TTL ?')
insert.consistency_level = ConsistencyLevel.QUORUM
insert_ttl.consistency_level = ConsistencyLevel.QUORUM

reading = True

f = src_session.execute_async(select)
w = None

count = 0

def continue_read():
    global reading
    if f.has_more_pages:
        f.start_fetching_next_page()
    reading = f.has_more_pages
        

def write_callback(result, rows):
    global count
    count += rows
    logging.info("Inserted %s rows " % count)
    continue_read()

def write_errback(exception):
    logging.error("Write error: %s" % exception)
    continue_read()

def read_callback(rows):
    global w
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM, batch_type=BatchType.UNLOGGED)
    for row in rows:
        if row.ttl_value:
            batch.add(insert_ttl, (row.key, row.column1, row.value, row.ttl_value))
        else:
            batch.add(insert, (row.key, row.column1, row.value))
    w = dst_session.execute_async(batch)
    w.add_callbacks(callback=write_callback, callback_kwargs={'rows': len(rows)}, errback=write_errback)

def read_errback(exception):
    logging.error("Read error: %s" % exception)
    time.sleep(5)
    continue_read()

f.add_callbacks(read_callback, read_errback)

while reading:
    time.sleep(1)

if w:
    w.result()
