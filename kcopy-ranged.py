#!/usr/bin/python

import pycassa, sys, time, logging
from pycassa.system_manager import *

FORMAT = "%(levelname)s %(asctime)-15s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG, filename='kcopy.log')

keyspace = 'KeyspaceName'
maxTTL = 2592000
maxRows = 10
maxColumnsPerRow = 4000
columnFamily = 'ColumnFamilyName'
startToken = None
source_host = sys.argv[1]
destinationCluster = ['192.168.1.1:9160','192.168.1.2:9160']
source = pycassa.ConnectionPool(keyspace=keyspace, server_list=[':'.join([source_host,'9160'])], prefill=False, max_retries=-1)
dest = pycassa.ConnectionPool(keyspace=keyspace, server_list=destinationCluster, prefill=False, max_retries=-1)

source_cf = pycassa.ColumnFamily(source, columnFamily)
dest_cf = pycassa.ColumnFamily(dest, columnFamily)

ranges = []
sm = SystemManager(source_host)
for token in sm.describe_ring(keyspace):
    if source_host == token.endpoints[0] and long(token.start_token) < long(token.end_token):
        ranges.append((token.start_token, token.end_token))
sm.close()
skip = True
total_keys = 0

def addColumn(ci, name, value, timestamp):
    life_timespan = (pycassa.columnfamily.gm_timestamp() - timestamp) / 1000000
    if maxTTL:
        ttl = maxTTL - life_timespan
        if ttl > 0:
            ci.update({name:value})
    else:
        ci.update({name:value})
    return ttl

def writeRows(rows, ttl):
    global total_keys
    while True:
        try:
            total_keys += len(rows)
            if maxTTL:
                dest_cf.batch_insert(rows, ttl=ttl, write_consistency_level=pycassa.ConsistencyLevel.ONE)
            else:
                dest_cf.batch_insert(rows, write_consistency_level=pycassa.ConsistencyLevel.ONE)
            logging.info("Inserted %s" % total_keys)
            break
        except:
            logging.error("Write exception: %s. Sleep 5 sec" % str(sys.exc_info()))
            time.sleep(5)

for start_token, finish_token in ranges:
    if startToken and start_token != startToken and skip:
        continue
    else:
        skip = False
    logging.info("process %s %s" % (start_token, finish_token))
    while True:
        try:
            rows = {}
            maxttl = 0
            for key, columns in source_cf.get_range(start_token=start_token, finish_token=finish_token, row_count=maxRows, column_count=maxColumnsPerRow, include_timestamp=True):
                ci = {}
                timestamp = 0
                if len(columns) < maxColumnsPerRow:
                    for name in columns:
                        value, timestamp = columns[name]
                        ttl = addColumn(ci, name, value, timestamp)
                        if ttl > maxttl:
                            maxttl = ttl
                else:
                    for c in source_cf.xget(key, include_timestamp=True):
                        name, v = c
                        value, timestamp = v
                        ttl = addColumn(ci, name, value, timestamp)
                        if ttl > maxttl:
                            maxttl = ttl
                        if len(ci) > maxColumnsPerRow:
                            writeRows({key:ci}, maxttl)
                            maxttl = 0
                            ci = {}
                if len(ci) > 0:
                    rows.update({key:ci})
                if len(rows) >= maxRows and maxttl > 0:
                    writeRows(rows, maxttl)
                    rows = {}
                    maxttl = 0
            break
        except:
            logging.error("Execute exception: %s. Sleep 5 sec" % str(sys.exc_info()))
            time.sleep(5)
logging.info("Copying %s completed" % columnFamily)
