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
import tempfile
import os
import random
import string

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
parser.add_argument('--offset', help="offset to begin inserts from from input file", type=int, default=0)
parser.add_argument('--binary-output-dir', help="directory name to output binary COPY statements to", default="/tmp/%s" % ''.join(random.choices(string.ascii_letters + string.digits, k=8)))
parser.add_argument('--binary-output-intermediate', help="save binary intermediate output. See also: --binary-output-dir. default: False", action='store_true')
parser.add_argument('--binary-input-dir', help="read COPY batches fron binary intermediate input. See also: --binary-output-intermediate.")

args = parser.parse_args()

conn = psycopg2.connect(database=args.db, user=args.db_user)

col_source = NARROW_FORMAT
if args.wide:
    col_source = WIDE_FORMAT

columns = [val for key, val in col_source.items() if not key.startswith("meta")]
columns.append("metadata") # in all formats and strategies, we use a special "metadata" column

if args.binary_output_intermediate:
    logging.warn("Saving binary COPY output to '%s'" % args.binary_output_dir)

managers = {
    'values': CopyManager(conn, args.values_table, columns)
}

timing_buckets = {
    "values_insert": {
        "total": 0.0,
        "count": 0,
        "min": None,
        "max": None,
    },
    "metadata_insert": {
        "total": 0.0,
        "count": 0,
        "min": None,
        "max": None,
    },
    "values_assembly": {
        "total": 0.0,
        "count": 0,
        "min": None,
        "max": None,
    },
    "metadata_assembly": {
        "total": 0.0,
        "count": 0,
        "min": None,
        "max": None,
    }
}

def get_file():
    fname = "%s.copy.bin%s" % (str(get_file.calls).zfill(8), get_file.suffix)
    get_file.calls += 1
    return open(os.path.join(get_file.dirname, fname), "wb+")

def tmpfile_factory(preserve_copy_files):
    tmpfile_factory = tempfile.TemporaryFile
    def tmpdir(dirname, suffix):
        if not hasattr(get_file, 'calls'):
            get_file.calls = 0
            get_file.dirname = dirname
        get_file.suffix = suffix
        return get_file
    if preserve_copy_files:
        tmpfile_factory = tmpdir(args.binary_output_dir, suffix)
    return tmpfile_factory

def timed_write_binary(mgr, batch, timing_bucket="values_write_binary", tmpfile_factory=tmpfile_factory(False), suffix=''):
    before = time.perf_counter()
    tmpfile = tmpfile_factory()
    get_tmpfile():
        return tmpfile
    mgr.copy(batch, get_tmpfile)
    # timing details
    after = time.perf_counter()
    execution_time = after - before
    timing_buckets[timing_bucket]["total"] += execution_time
    if timing_buckets[timing_bucket]["min"] is None or execution_time < timing_buckets[timing_bucket]["min"]:
        timing_buckets[timing_bucket]["min"] = execution_time
    if timing_buckets[timing_bucket]["max"] is None or execution_time > timing_buckets[timing_bucket]["max"]:
        timing_buckets[timing_bucket]["max"] = execution_time
    timing_buckets[timing_bucket]["count"] += 1
    return tmpfile

def timed_copy_binary(f, filename, timing_bucket="values_insert"):
    before = time.perf_counter()
    if 'metadata' in filename:
        with conn.cursor() as cur:
            # create temp table
            cur.execute("CREATE TEMP TABLE tmp_table (data jsonb) ON COMMIT DROP")
            # do COPY
            tmp_mgr = CopyManager(conn, 'tmp_table', ("data",))
            tmp_mgr.copystream(f)
            # insert from temp table into the metadata table
            cur.execute("INSERT INTO %s SELECT * FROM tmp_table ON CONFLICT DO NOTHING" % args.metadata_table)
        conn.commit()
        logging.info('copied %s metadata rows (postgres insert time)' % args.batch_size)
    else:
        managers['values'].copystream(f)
        logging.info('copied %s values rows (postgres insert time)' % args.batch_size)
    after = time.perf_counter()
    execution_time = after - before
    timing_buckets[timing_bucket]["total"] += execution_time
    if timing_buckets[timing_bucket]["min"] is None or execution_time < timing_buckets[timing_bucket]["min"]:
        timing_buckets[timing_bucket]["min"] = execution_time
    if timing_buckets[timing_bucket]["max"] is None or execution_time > timing_buckets[timing_bucket]["max"]:
        timing_buckets[timing_bucket]["max"] = execution_time
    timing_buckets[timing_bucket]["count"] += 1


def insert_batch(batch, strategy="hashed-metadata"):
    if strategy == "hashed-metadata":
        # i[-1] because last column is 'metadata'
        metadata_batch = [(i[-1],) for i in batch]
        with conn.cursor() as cur:
            # create temp table
            cur.execute("CREATE TEMP TABLE tmp_table (data jsonb) ON COMMIT DROP")
            # do COPY
            mgr = CopyManager(conn, 'tmp_table', ("data",))
            tmpfile = timed_write_binary(mgr, metadata_batch, timing_bucket="metadata_write_binary", tmpfile_factory=tmpfile_factory(preserve_copy_files=args.binary_output_intermediate), suffix='.metadata')
            timed_copy_binary(tmpfile, tmpfile.filename, timing_bucket="metadata_insert")
            # insert from temp table into the metadata table
            cur.execute("INSERT INTO %s SELECT * FROM tmp_table ON CONFLICT DO NOTHING" % args.metadata_table)
        # end transaction
        logging.info('copied %s metadata rows (postgres insert time)' % len(batch))
        conn.commit()
        logging.info('committed %s metadata rows (postgres overhead)' % len(batch))
        tmpfile = timed_write_binary(managers['values'], batch, timing_bucket="values_write_binary", tmpfile_factory=tmpfile_factory(preserve_copy_files=args.binary_output_intermediate))
        timed_copy_binary(tmpfile, tmpfile.filename, timing_bucket="values_insert")
        logging.info('copied %s values rows (postgres insert time)' % len(batch))
    if strategy == "inline-metadata":
        tmpfile = timed_write_binary(managers['values'], batch, timing_bucket="values_write_binary", tmpfile_factory=tmpfile_factory(preserve_copy_files=args.binary_output_intermediate))
        timed_copy_binary(tmpfile, tmpfile.filename, timing_bucket="values_insert")
        logging.info('copied %s values rows (postgres insert time)' % len(batch))

def timed_assembly(infile, header, batch_size=1, timing_bucket="values_assembly", offset=0):
    batch = []
    before = time.perf_counter()
    curr_line = 0
    for line in infile:
        curr_line += 1
        if curr_line < offset:
            if curr_line % 1000 == 0:
                logging.info("seeking to offset... %s", curr_line)
            continue
        row = line.rstrip("\n").split("\t")

        batch.append(assemble(row, header, fmt=WIDE_FORMAT if args.wide else NARROW_FORMAT, original_line=line))
        if len(batch) == args.batch_size:
            logging.info('assembled %s values rows (python assembly overhead)' % args.batch_size)
            
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

def final_report():
    batch_stats_template = """
    Batch Statistics (%s):
        Total batches: %s
        Rows per batch: %s
        Average batch conversion/assembly overhead: %.2fs
        Average batch insertion time: %.2fs
        Min batch insertion time: %.2fs
        Max batch insertion time: %.2fs
    """
    total_times_template = """
    Total insertion time (%s): %.2fs
    """

    reports = ["values"]
    if args.strategy == "":
        reports.append("metadata")

    batch_stats = ""
    total_times = ""
    for report in reports:
        batch_stats += batch_stats_template % (
            report,
            timing_buckets["%s_assembly" % report]["count"],
            args.batch_size,
            timing_buckets["%s_assembly" % report]["total"] / (timing_buckets["%s_assembly" % report]["count"] or 1),
            timing_buckets["%s_insert" % report]["total"] / (timing_buckets["%s_insert" % report]["count"] or 1),
            timing_buckets["%s_insert" % report]["min"],
            timing_buckets["%s_insert" % report]["max"],
        )
        total_times += total_times_template % (
            report,
            timing_buckets["%s_insert" % report]["total"]
        )

    avg_insertion_rate = total_inserts / (timing_buckets["values_insert"]["total"] or 1)
    
    print("""

    --- Insertion Summary ---
    Total rows processed: %s
    %s
    Average insertion rate: %.2fs rows/sec
    %s
    """ % (total_inserts, total_times, avg_insertion_rate, batch_stats))


        
total_inserts = 0

if args.binary_input_dir:
    for filename in sorted(os.listdir(args.binary_input_dir)):
        with open(os.path.join(args.binary_input_dir, filename), 'rb') as f:
            copy_binary_batch(f, filename, timing_bucket='values_insert' if 'metadata' in filename else 'metadata_insert')
            if 'metadata' not in filename:
                total_inserts += args.batch_size
else:
    header_line = args.infile.readline()
    header = header_line.strip().split("\t")
    for batch in timed_assembly(infile=args.infile, header=header, batch_size=args.batch_size, timing_bucket="values_assembly", offset=args.offset):
        insert_batch(batch, strategy=args.strategy)
        total_inserts += len(batch)
        if total_inserts >= args.limit:
            conn.commit()
            logging.info('committed %s values rows (postgres overhead)' % args.limit)
            break


# don't forget to commit!
conn.commit()
logging.info('committed %s values rows (postgres overhead)' % total_inserts)
conn.close()
final_report()
