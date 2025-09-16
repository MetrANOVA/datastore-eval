import argparse
from csv_format import WIDE_FORMAT
import csv
import os
import subprocess 
import random
import string
import sys
import logging
import datetime

parser = argparse.ArgumentParser(description='Inserts ESnet Stardust Data into victoriametrics, producing a timing summary report.')

parser.add_argument('--host', help="Remote VictoriaMetrics host")
parser.add_argument('--port', help='Remote VictoriaMetrics port')
parser.add_argument('--wide', help='Stardust wide snmp format', action='store_true')
parser.add_argument('--flow', help='Stardust flow format', action='store_true')
parser.add_argument('--split', help='Split TSV into batches', action='store_true')
parser.add_argument('--insert', help='Do inserts from split CSV', action='store_true')
parser.add_argument('--output-dir', help='write batches/load batches from directory', default="/tmp/%s" % ''.join(random.choices(string.ascii_letters + string.digits, k=8)))
parser.add_argument('--infile', help="input TSV file. Default: sys.stdin", default=sys.stdin, type=argparse.FileType('r'))
parser.add_argument('--batch-size', help='insert batch size', type=int, default=10000)
parser.add_argument('--workers', help="total number of workers", default=10)
parser.add_argument('--worker', help='number of this worker')
parser.add_argument('--limit', help='row limit', type=int, default=100000000)

args = parser.parse_args()

if args.split:
    worker_log_string = "[splitter]"
else:
    worker_log_string = "[%s/%s]" % (args.worker, args.workers)
    
logging.basicConfig(format=f'%(asctime)s :: {worker_log_string} :: %(message)s', level=logging.INFO)

if args.flow:
    sys.exit("flow format is not yet implemented (csv_format.py)")

if args.wide and args.flow:
    sys.exit("wide and flow format are mutually exclusive")

if args.insert and args.split:
    sys.exit("insert and split are mutually exclusive. The program either splits data or inserts it.")

def get_outfile(worker):
    fname = "%s.copy.%s" % (str(get_outfile.calls).zfill(8), get_outfile.suffix)
    get_outfile.calls += 1
    os.makedirs(os.path.join(args.output_dir, f"{worker}"), exist_ok=True)
    return open(os.path.join(args.output_dir, f"{worker}", fname), "w")
get_outfile.calls = 0
get_outfile.suffix = 'csv'

def write_csv(header, batch):
    worker = write_csv.calls % args.workers
    outfile = get_outfile(worker)
    logging.info("Writing Batch to %s..." % outfile.name)
    writer = csv.writer(outfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
    writer.writerow(header)
    for row in batch:
        writer.writerow(row)
    write_csv.calls += 1
write_csv.calls = 0

def process_line(line, last_metric):
    return [float(v) if i<=last_metric else v for i, v in enumerate(line)]


if args.split:
    batch = []
    i = 0
    dialect = csv.Sniffer().sniff(args.infile.read(1024))
    reader = csv.reader(args.infile, dialect)
    header = reader.__next__()
    last_metric = -1
    if args.wide:
        for t in WIDE_FORMAT:
            if t[1] == 'metric':
                last_metric += 1
    for line in reader:
        if i % 1000 == 0:
            logging.info("processing line %s" % i)
        if len(batch) == args.batch_size:
            write_csv(header, batch)
            batch = []
            if i >= args.limit:
                sys.exit("completed batching.")
        batch.append(process_line(line, last_metric))
        i += 1

if args.insert:
    for ff in os.listdir(args.output_dir):
        filename = os.path.join(args.output_dir, ff)
        start_time = datetime.datetime.now()
        if args.wide:
            format_string = ",".join([":".join(t) for t in WIDE_FORMAT])
#        if args.flow:
#            format_string = ",".join([":".join(t) for t in FLOW_FORMAT])
        cmd = ["curl", "-X", "POST", f"http://{args.host}:{args.port}/api/v1/import/csv?format={format_string}", "-T", filename]
        logging.debug(" ".join(cmd))
        output = subprocess.run(cmd, capture_output=True)
        #logging.debug(output)
        logging.info("inserted batch of %s records from %s", args.batch_size, filename)
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        scoreboard_format = "1:time:rfc3339,2:metric:duration,3:metric:batch_size,4:metric:data_type"
        scoreboard_row = f"{start_time},{duration},{args.batch_size},scoreboard"
        output = subprocess.run(["curl", "-d", ",".join(scoreboard_row), f"http://{args.host}:{args.port}/api/v1/import/csv?format={scoreboard_format}"], capture_output=True)
        logging.debug(output)
        
