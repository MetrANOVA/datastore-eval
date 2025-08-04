import argparse
import csv
import json
import datetime
from mappings import NARROW_FORMAT, WIDE_FORMAT
from datetime import datetime
from assemble import assemble
import logging
import tempfile
import os
import random
import string
import hashlib
import pickle
import sys
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import time

logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)

parser = argparse.ArgumentParser(description='Inserts ESnet Stardust Data into elasticsearch.')

# connection
parser.add_argument('--user', help='Elasticsearch username')
parser.add_argument('--password', help='Elasticsearch User Password')
parser.add_argument('--host', help="remote elastic host", default='localhost')
parser.add_argument('--port', help="remote elastic port", default='9200')

# indexes to use
parser.add_argument('--values-index', help="Table to insert values into. Receives both values and metadata (in 'metadata' column) when strategy is 'inline-metadata'", default='metranova_values')
parser.add_argument('--scoreboard-index', help="Table to insert metrics/scoreboard info.", default='scoreboard')

# partitioning and insert options
parser.add_argument('--total-partitions', help="use consistent hash partitioning to partition binary output results", type=int, default=0)
parser.add_argument('--partition', help="the binary output partition to prepare", type=int)
parser.add_argument('--skip', help="Only insert every Nth row", type=int, default=1)
parser.add_argument('--offset', help="offset to begin inserts from from input file", type=int, default=0)
parser.add_argument('--limit', help="total insertion limit", type=int, default=20000)
parser.add_argument('--batch-size', help="Batch size to do inserts, in rows.", type=int, default=5000)

# input file and format
parser.add_argument('--infile', help="Read rows from infile. Default: sys.stdin", default=sys.stdin, type=argparse.FileType('r'))
parser.add_argument('--wide', help="Use stardust 'wide' format, including all columns.", action='store_true')

# intermediate transform output
parser.add_argument('--transform-output-dir', help="directory name to output binary COPY statements to", default="/tmp/%s" % ''.join(random.choices(string.ascii_letters + string.digits, k=8)))
parser.add_argument('--transform-output-intermediate', help="save binary intermediate output. See also: --transform-output-dir. default: False", action='store_true')
parser.add_argument('--transform-input-dir', help="read COPY batches fron binary intermediate input. See also: --transform-output-intermediate.")

arguments = parser.parse_args()
basic_auth = None
if arguments.user and arguments.password:
    basic_auth = (arguments.user, arguments.password)
es_client = Elasticsearch(hosts=["http://%s:%s" % (arguments.host, arguments.port)], basic_auth=basic_auth)

col_source = NARROW_FORMAT
if arguments.wide:
    col_source = WIDE_FORMAT

timing_buckets = {
    "insert": {
        "total": 0.0,
        "count": 0,
        "min": None,
        "max": None,
    },
    "write_transformed": {
        "total": 0.0,
        "count": 0,
        "min": None,
        "max": None,
    },
    "assembly": {
        "total": 0.0,
        "count": 0,
        "min": None,
        "max": None,
    },
}

# Assemble document rows

def timed_assembly(infile, header, batch_size=1, timing_bucket="assembly", offset=0):
    batch = []
    before = time.perf_counter()
    curr_line = 0
    for line in infile:
        if curr_line < offset or ((curr_line - offset) % arguments.skip) != 0:
            if curr_line < offset and curr_line % 1000 == 0:
                logging.info("seeking to offset... %s", curr_line)
            elif curr_line < 1000 and ((curr_line - offset) % arguments.skip) != 0:
                logging.info("(Batching Debug) skipping line %s. Offset: %s Skip: %s Curr_line - offset: %s Curr_line - offset mod skip: %s" % (curr_line, offset, arguments.skip, (curr_line - offset), ((curr_line - offset) % arguments.skip) ))
            curr_line += 1
            continue
        curr_line += 1
        row = line.rstrip("\n").split("\t")
        if arguments.total_partitions:
            hash_output = hash_row(row)
            hash_bucket = int(hash_output, 16) % arguments.total_partitions
            if hash_bucket != arguments.partition:
                continue
            if curr_line % 1000 == 0:
                logging.info("row %s: partition %s" % (curr_line, hash_bucket))

        batch.append(assemble(row, header, fmt=WIDE_FORMAT if arguments.wide else NARROW_FORMAT, original_line=line))
        if len(batch) == arguments.batch_size:
            logging.info('assembled %s values rows (python assembly overhead)' % arguments.batch_size)
            
            # timing details
            after = time.perf_counter()
            execution_time = after - before
            timing_buckets[timing_bucket]["total"] += execution_time
            if timing_buckets[timing_bucket]["min"] is None or execution_time < timing_buckets[timing_bucket]["min"]:
                timing_buckets[timing_bucket]["min"] = execution_time
            if timing_buckets[timing_bucket]["max"] is None or execution_time > timing_buckets[timing_bucket]["max"]:
                timing_buckets[timing_bucket]["max"] = execution_time
            timing_buckets[timing_bucket]["count"] += 1
            yield batch
            batch = []
            before = time.perf_counter()
    
    # yield the last incomplete batch, don't both with min/max statistics
    after = time.perf_counter()
    timing_buckets[timing_bucket]["total"] += execution_time
    timing_buckets[timing_bucket]["count"] += 1
    yield batch

# Deal with details of transformed output

def get_file():
    fname = "%s.transformed.%s" % (str(get_file.calls).zfill(8), get_file.suffix)
    get_file.calls += 1
    return open(os.path.join(get_file.dirname, fname), "wb+")

def tmpfile_factory(preserve_files, suffix="json"):
    tmpfile_factory = lambda: tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    def tmpdir(dirname, suffix):
        if not hasattr(get_file, 'calls'):
            get_file.calls = 0
            get_file.dirname = dirname
        get_file.suffix = suffix
        return get_file
    if preserve_files:
        tmpfile_factory = tmpdir(arguments.transform_output_dir, suffix)
    return tmpfile_factory

def timed_write_transformed(batch, timing_bucket="write_transformed", factory=tmpfile_factory(False)):
    before = time.perf_counter()
    tmpfile = factory()
    pickle.dump(batch, tmpfile, protocol=pickle.HIGHEST_PROTOCOL)
    # timing details
    after = time.perf_counter()
    execution_time = after - before
    timing_buckets[timing_bucket]["total"] += execution_time
    if timing_buckets[timing_bucket]["min"] is None or execution_time < timing_buckets[timing_bucket]["min"]:
        timing_buckets[timing_bucket]["min"] = execution_time
    if timing_buckets[timing_bucket]["max"] is None or execution_time > timing_buckets[timing_bucket]["max"]:
        timing_buckets[timing_bucket]["max"] = execution_time
    timing_buckets[timing_bucket]["count"] += 1
    tmpfile = open(tmpfile.name, 'rb')
    return tmpfile

def timed_bulk_insert(f, timing_bucket="insert"):
    batch = pickle.load(f)
    before = time.perf_counter()
    before_timestamp = datetime.now()
    elasticsearch.helpers.bulk(es_client, batch, index=arguments.values_index)
    logging.info('.bulk() %s rows (elasticsearch insert time)' % arguments.batch_size)
    after = time.perf_counter()
    after_timestamp = datetime.now()
    execution_time = after - before
    timing_buckets[timing_bucket]["total"] += execution_time
    if timing_buckets[timing_bucket]["min"] is None or execution_time < timing_buckets[timing_bucket]["min"]:
        timing_buckets[timing_bucket]["min"] = execution_time
    if timing_buckets[timing_bucket]["max"] is None or execution_time > timing_buckets[timing_bucket]["max"]:
        timing_buckets[timing_bucket]["max"] = execution_time
    timing_buckets[timing_bucket]["count"] += 1
    es_client.index(document={
        "index_name": arguments.values_index, 
        "batch_size": arguments.batch_size,
        "start_time": before_timestamp.isoformat(),
        "end_time": after_timestamp.isoformat()
    }, index=arguments.scoreboard_index)

def hash_row(row):
    ROUTER_IDX = 19
    PORT_IDX = 34
    if arguments.wide:
        ROUTER_IDX = 605
        PORT_IDX = 610
    port_string = "%s::%s" % (row[ROUTER_IDX], row[PORT_IDX])
    return hashlib.md5(port_string.encode('UTF-8')).hexdigest()

total_inserts = 0

if arguments.transform_input_dir:
    for filename in sorted(os.listdir(arguments.transform_input_dir)):
        with open(os.path.join(arguments.transform_input_dir, filename), 'rb') as f:
            timed_bulk_insert(f)
            total_inserts += arguments.batch_size
else:
    header_line = arguments.infile.readline()
    header = header_line.strip().split("\t")
    logging.info('about to call timed_assembly')
    batches = timed_assembly(infile=arguments.infile, header=header, batch_size=arguments.batch_size, timing_bucket="assembly", offset=arguments.offset)
    for batch in batches:
        total_inserts += len(batch)
        logging.info("assembled %s rows" % len(batch))
        timed_write_transformed(batch, factory=tmpfile_factory(preserve_files=arguments.transform_output_intermediate))
        if total_inserts >= arguments.limit:
            break
