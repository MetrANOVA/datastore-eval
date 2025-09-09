import argparse
import csv
import json
import datetime
from mappings import NARROW_FORMAT, WIDE_FORMAT, WIDE_NORMALIZED_FORMAT, FLOW_FORMAT
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
import hashlib


parser = argparse.ArgumentParser(description='Inserts ESnet Stardust Data into timescaledb, producing a timing summary report.')

parser.add_argument('--db', help='postgres database. Connections assumed to be on a unix domain socket.', default="timescale")
parser.add_argument('--db-user', help='postgres user. This user is assumed to be a superuser and to be authenticated via "trust" (via unix domain socket)', default='timescale')
parser.add_argument('--values-table', help="Table to insert values into. Receives both values and metadata (in 'metadata' column) when strategy is 'inline-metadata'", default='values')
parser.add_argument('--metadata-table', help="Table to insert metadata into. Only used when strategy is 'hashed-metadata'", default='metadata')
parser.add_argument('--scoreboard-table', help="Table to insert metrics/scoreboard info.", default='scoreboard')
parser.add_argument('--strategy', help='metadata insertion strategy. Options are "hashed-metadata" or "inline-metadata".'
                    ' When "hashed-metadata", metadata will be inserted into the "metadata-table" and referenced via hash.'
                    ' When "inline-metadata", metadata objects will be inserted into the same row as values.', default="hashed-metadata")
parser.add_argument('--infile', help="Read rows from infile. Default: sys.stdin", default=sys.stdin, type=argparse.FileType('r'))
parser.add_argument('--wide', help="Use stardust 'wide' format, including all columns.", action='store_true')
parser.add_argument('--flow', help="Use stardust 'flow' format, including all flow columns.", action='store_true')
parser.add_argument('--normalized', help="For 'wide' format, normalize metadata into columns.", action='store_true')
parser.add_argument('--batch-size', help="Batch size to do inserts, in rows.", type=int, default=5000)
parser.add_argument('--limit', help="total insertion limit", type=int, default=20000)
parser.add_argument('--skip', help="Only insert every Nth row", type=int, default=1)
parser.add_argument('--offset', help="offset to begin inserts from from input file", type=int, default=0)
parser.add_argument('--binary-output-dir', help="directory name to output binary COPY statements to", default="/tmp/%s" % ''.join(random.choices(string.ascii_letters + string.digits, k=8)))
parser.add_argument('--binary-output-intermediate', help="save binary intermediate output. See also: --binary-output-dir. default: False", action='store_true')
parser.add_argument('--binary-input-dir', help="read COPY batches fron binary intermediate input. See also: --binary-output-intermediate.")
parser.add_argument('--host', help="remote postgres host")
parser.add_argument('--total-partitions', help="use consistent hash partitioning to partition binary output results", type=int, default=0)
parser.add_argument('--partition', help="the binary output partition to prepare", type=int)


args = parser.parse_args()

worker_log_string = "[%s/%s]" % (args.offset + 1, args.skip)
if args.partition:
    worker_log_string = "[%s/%s]" % (args.partition, args.total_partitions)
    
logging.basicConfig(format=f'%(asctime)s :: {worker_log_string} :: %(message)s', level=logging.INFO)

conn = psycopg2.connect(database=args.db, user=args.db_user, host=args.host, port=5432)

col_source = NARROW_FORMAT
if args.wide:
    col_source = WIDE_FORMAT
    if args.wide and args.normalized:
        col_source = WIDE_NORMALIZED_FORMAT
if args.flow:
    col_source = FLOW_FORMAT

columns = [val for key, val in col_source.items() if not key.startswith("meta")]
columns.append("metadata") # in all formats and strategies, we use a special "metadata" column
if args.normalized:
    columns = [val for key, val in col_source.items() if not key.startswith("values.queue")]
    columns.append("queues")
if args.flow:
    columns = [val for key, val in col_source.items() if not val.startswith("bgp") and not val.startswith("esdb") and not val.startswith("mpls") and not val.startswith("scireg")]
    columns.append("bgp")
    columns.append("esdb")
    columns.append("mpls")
    columns.append("scireg")

    
if args.binary_output_intermediate:
    logging.warn("Saving binary COPY output to '%s'" % (args.binary_output_dir))

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
    "values_write_binary": {
        "total": 0.0,
        "count": 0,
        "min": None,
        "max": None,
    },
    "metadata_write_binary": {
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

def tmpfile_factory(preserve_copy_files, suffix="values"):
    tmpfile_factory = lambda: tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    def tmpdir(dirname, suffix):
        if not hasattr(get_file, 'calls'):
            get_file.calls = 0
            get_file.dirname = dirname
        get_file.suffix = suffix
        return get_file
    if preserve_copy_files:
        tmpfile_factory = tmpdir(args.binary_output_dir, suffix)
    return tmpfile_factory

def timed_write_binary(mgr, batch, timing_bucket="values_write_binary", factory=tmpfile_factory(False)):
    before = time.perf_counter()
    tmpfile = factory()
    def get_tmpfile():
        return tmpfile
    try:
        mgr.copy(batch, get_tmpfile)
    except Exception as e:
        import pdb; pdb.set_trace()
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

def timed_copy_binary(mgr, f, filename, timing_bucket="values_insert"):
    before = time.perf_counter()
    before_timestamp = datetime.now()
    mgr.copystream(f)
    logging.info('COPY %s rows (postgres insert time)' % args.batch_size)
    after = time.perf_counter()
    after_timestamp = datetime.now()
    execution_time = after - before
    timing_buckets[timing_bucket]["total"] += execution_time
    if timing_buckets[timing_bucket]["min"] is None or execution_time < timing_buckets[timing_bucket]["min"]:
        timing_buckets[timing_bucket]["min"] = execution_time
    if timing_buckets[timing_bucket]["max"] is None or execution_time > timing_buckets[timing_bucket]["max"]:
        timing_buckets[timing_bucket]["max"] = execution_time
    timing_buckets[timing_bucket]["count"] += 1
    with conn.cursor() as cur:
        cur.execute('''INSERT INTO %s (table_name, batch_size, start_time, end_time) VALUES ('%s', %s, '%s', '%s')''' % (
            args.scoreboard_table, args.values_table, args.batch_size, before_timestamp.isoformat(), after_timestamp.isoformat()
        ))
    conn.commit()


def insert_batch(batch, strategy="hashed-metadata"):
    if strategy == "hashed-metadata":
        # i[-1] because last column is 'metadata'
        metadata_batch = [(i[-1],) for i in batch]
        with conn.cursor() as cur:
            # create temp table
            cur.execute("CREATE TEMP TABLE tmp_table (data jsonb) ON COMMIT DROP")
            # do COPY
            mgr = CopyManager(conn, 'tmp_table', ("data",))
            tmpfile = timed_write_binary(mgr, metadata_batch, timing_bucket="metadata_write_binary", factory=tmpfile_factory(preserve_copy_files=args.binary_output_intermediate, suffix='.metadata'))
            logging.info('Wrote binary output %s metadata rows (intermediate filesystem write time)' % len(batch))
            timed_copy_binary(mgr, tmpfile, tmpfile.name, timing_bucket="metadata_insert")
            # insert from temp table into the metadata table
            cur.execute("INSERT INTO %s SELECT * FROM tmp_table ON CONFLICT DO NOTHING" % args.metadata_table)
        # end transaction
        conn.commit()
        logging.info('Completed commit for %s metadata rows (postgres overhead)' % len(batch))
        tmpfile = timed_write_binary(managers['values'], batch, timing_bucket="values_write_binary", factory=tmpfile_factory(preserve_copy_files=args.binary_output_intermediate, suffix='.values'))
        logging.info('Wrote binary output for %s values rows (intermediate filesystem write time)' % len(batch))
        timed_copy_binary(managers['values'], tmpfile, tmpfile.name, timing_bucket="values_insert")
    if strategy == "inline-metadata":
        tmpfile = timed_write_binary(managers['values'], batch, timing_bucket="values_write_binary", factory=tmpfile_factory(preserve_copy_files=args.binary_output_intermediate, suffix='.values'))
        logging.info('Wrote binary output for %s values rows (intermediate filesystem write time)' % len(batch))
        timed_copy_binary(managers['values'], tmpfile, tmpfile.name, timing_bucket="values_insert")

def copy_batch(tmpfile, tmpfile_name):
    if "metadata" in tmpfile_name:
        with conn.cursor() as cur:
            # create temp table
            cur.execute("CREATE TEMP TABLE tmp_table (data jsonb) ON COMMIT DROP")
            # do COPY
            mgr = CopyManager(conn, 'tmp_table', ("data",))
            timed_copy_binary(mgr, tmpfile, tmpfile_name, timing_bucket="metadata_insert")
            # insert from temp table into the metadata table
            cur.execute("INSERT INTO %s SELECT * FROM tmp_table ON CONFLICT DO NOTHING" % args.metadata_table)
        # end transaction
        conn.commit()
        logging.info('Completed commit for %s metadata rows (postgres overhead)' % args.batch_size)
    else:
        timed_copy_binary(managers['values'], tmpfile, tmpfile_name, timing_bucket="values_insert")

def hash_row(row):
    ROUTER_IDX = 19
    PORT_IDX = 34
    if args.wide:
        ROUTER_IDX = 605
        PORT_IDX = 610
    port_string = "%s::%s" % (row[ROUTER_IDX], row[PORT_IDX])
    return hashlib.md5(port_string.encode('UTF-8')).hexdigest()
        
def timed_assembly(infile, header, batch_size=1, timing_bucket="values_assembly", offset=0):
    batch = []
    before = time.perf_counter()
    curr_line = 0
    fmt = NARROW_FORMAT
    if args.wide:
        fmt = WIDE_FORMAT
        if args.normalized:
            fmt = WIDE_NORMALIZED_FORMAT
    if args.flow:
        fmt = FLOW_FORMAT
    for line in infile:
        if curr_line < offset or ((curr_line - offset) % args.skip) != 0:
            if curr_line % 1000 == 0:
                logging.info("seeking to offset... %s", curr_line)
            elif curr_line < 1000 and ((curr_line - offset) % args.skip) != 0:
                logging.info("skipping line %s. Offset: %s Skip: %s Curr_line - offset: %s Curr_line - offset mod skip: %s" % (curr_line, offset, args.skip, (curr_line - offset), ((curr_line - offset) % args.skip) ))
            curr_line += 1
            continue
        curr_line += 1
        row = line.rstrip("\n").split("\t")
        if args.total_partitions:
            hash_output = hash_row(row)
            hash_bucket = int(hash_output, 16) % args.total_partitions
            if hash_bucket != args.partition:
                continue
            if curr_line % 1000 == 0:
                logging.info("row %s: partition %s" % (curr_line, hash_bucket))
        try:
            batch.append(assemble(row, header, fmt=fmt, original_line=line, normalized=args.normalized, flow=args.flow))
        except Exception as e:
            logging.error("error assembling row! %s" % e)
            continue
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
    Average insertion rate: %.0f rows/sec
    %s
    """ % (total_inserts, total_times, avg_insertion_rate, batch_stats))


        
total_inserts = 0

if args.binary_input_dir:
    for filename in sorted(os.listdir(args.binary_input_dir)):
        with open(os.path.join(args.binary_input_dir, filename), 'rb') as f:
            copy_batch(f, filename)
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
