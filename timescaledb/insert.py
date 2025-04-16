import argparse
import csv
import json
import datetime
from mappings import STARDUST_NARROW, STARDUST_WIDE
from datetime import datetime
from pgcopy import CopyManager
import psycopg2

parser = argparse.ArgumentParser(description='Inserts ESnet Stardust into timescaledb')

parser.add_argument('--db', help='postgres database', default="timescale")
parser.add_argument('--values-table', default='values')
parser.add_argument('--metadata-table', default='metadata')
parser.add_argument('--strategy', default="hashed-metadata")
parser.add_argument('--format', default="stardust-wide")
parser.add_argument('--infile', default=sys.stdin, type=argparse.FileType('r'))
parser.add_argument('--wide', action='store_true')
parser.add_argument('--batch-size', default=5000)

conn = psycopg2.connect(database='weather_db')
managers = {
    "values": CopyManager(conn, args.values_table, cols),
}

col_source = STARDUST_NARROW
if args.format == "stardust-wide":
    col_source = STARDUST_WIDE

columns = [val for key, val in col_source.items() if not key.startswith("meta")]
columns.push("metadata") # in all formats and strategies, we use a special "metadata" column

managers = {
    'values': CopyManager(conn, args.values_table, columns)
}
if args.strategy == "hashed-metadata":
    managers['metadata'] = CopyManager(conn, args.metadata_table, ("data"))

header = args.infile.readline().split("\t")
batch = []
for row in args.infile.readline():
    row = row.split("\t")
    values, metadata = assemble(row)
    batch.push((values, metadata))
    if len(batch) == args.batch_size:
        if args.strategy == "hashed-metadata":
            manager['metadata'].copy([i[1] for i in batch])
            manager['values'].copy([i[0] for i in batch])
        if args.strategy == "inline-metadata":
            manager['values'].copy([i[0] for i in batch])
        batch = []
# finish last batch
if args.strategy == "hashed-metadata":
    manager['metadata'].copy([i[1] for i in batch])
    manager['values'].copy([i[0] for i in batch])
if args.strategy == "inline-metadata":
    manager['values'].copy([i[0] for i in batch])


# don't forget to commit!
conn.commit()
