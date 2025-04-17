import argparse
import csv
import json
import datetime
from mappings import NARROW_FORMAT, WIDE_FORMAT
from datetime import datetime
from pgcopy import CopyManager
import psycopg2
import sys
import time
from assemble import assemble
import logging

logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)

parser = argparse.ArgumentParser(description='Inserts ESnet Stardust Data into timescaledb, producing a timing summary report.')

parser.add_argument('--db', help='postgres database. Connections assumed to be on a unix domain socket.', default="timescale")
parser.add_argument('--db-user', help='postgres user. This user is assumed to be a superuser and to be authenticated via "trust" (via unix domain socket)', default='timescale')
parser.add_argument('--values-table', help="Table to insert values into. Receives both values and metadata (in 'metadata' column) when strategy is 'inline-metadata'", default='values')
parser.add_argument('--metadata-table', help="Table to insert metadata into. Only used when strategy is 'hashed-metadata'", default='metadata')
parser.add_argument('--strategy', help='metadata insertion strategy. Options are "hashed-metadata" or "inline-metadata".'
                    ' When "hashed-metadata", metadata will be inserted into the "metadata-table" and referenced via hash.'
                    ' When "inline-metadata", metadata objects will be inserted into the same row as values.', default="hashed-metadata")
parser.add_argument('--infile', help="Read rows from infile. Default: sys.stdin", default=sys.stdin, type=argparse.FileType('r'))
parser.add_argument('--wide', help="Use stardust 'wide' format, including all columns.", action='store_true')
parser.add_argument('--batch-size', help="Batch size to do inserts, in rows.", type=int, default=5000)
parser.add_argument('--limit', help="total insertion limit", type=int, default=20000)

args = parser.parse_args()

conn = psycopg2.connect(database=args.db, user=args.db_user)

col_source = NARROW_FORMAT
if args.wide:
    col_source = WIDE_FORMAT

columns = [val for key, val in col_source.items() if not key.startswith("meta")]
columns.append("metadata") # in all formats and strategies, we use a special "metadata" column

managers = {
    'values': CopyManager(conn, args.values_table, columns)
}

timing_buckets = {
    "values": 0.0,
    "metadata": 0.0,
}

def timed_copy(mgr, batch, timing_bucket="values"):
    before = time.perf_counter()
    mgr.copy(batch)
    after = time.perf_counter()
    timing_buckets[timing_bucket] += after - before
    
def insert_batch(batch, strategy="hashed-metadata"):
    if strategy == "hashed-metadata":
        # i[-1] because last column is 'metadata'
        metadata_batch = [(i[-1],) for i in batch]
        with conn.cursor() as cur:
            # create temp table
            cur.execute("CREATE TEMP TABLE tmp_table (data jsonb) ON COMMIT DROP")
            # do COPY
            mgr = CopyManager(conn, 'tmp_table', ("data",))
            timed_copy(mgr, metadata_batch, timing_bucket="metadata")
            # insert from temp table into the metadata table
            cur.execute("INSERT INTO %s SELECT * FROM tmp_table ON CONFLICT DO NOTHING" % args.metadata_table)
        # end transaction
        logging.info('copied %s metadata rows (postgres insert time)' % len(batch))
        conn.commit()
        logging.info('committed %s metadata rows (postgres overhead)' % len(batch))
        timed_copy(managers['values'], batch, timing_bucket="values")
        logging.info('copied %s values rows (postgres insert time)' % len(batch))
    if strategy == "inline-metadata":
        timed_copy(managers['values'], batch, timing_bucket="values")
        logging.info('copied %s values rows (postgres insert time)' % len(batch))
    
header_line = args.infile.readline()
header = header_line.strip().split("\t")
total_inserts = 0
batch = []
for line in args.infile:
    row = line.rstrip("\n").split("\t")
    batch.append(assemble(row, header, fmt=WIDE_FORMAT if args.wide else NARROW_FORMAT, original_line=line))
    if len(batch) == args.batch_size:
        logging.info('assembled %s values rows (python assembly overhead)' % args.batch_size)
        insert_batch(batch, strategy=args.strategy)
        total_inserts += len(batch)
        if total_inserts >= args.limit:
            conn.commit()
            logging.info('committed %s values rows (postgres overhead)' % args.limit)
            conn.close()
            sys.exit()
        batch = []
# finish last batch
total_inserts += len(batch)
insert_batch(batch, strategy=args.strategy)

# 
# don't forget to commit!
conn.commit()
logging.info('committed %s values rows (postgres overhead)' % total_inserts)
conn.close()
