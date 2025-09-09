import argparse
import csv
import json
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import sys
from formats import WIDE_FORMAT, NARROW_FORMAT, FLOW_FORMAT
import logging

parser = argparse.ArgumentParser(description='Fetches public data from ESnet Stardust, formatting the output as csv, tsv, or json.')

now = datetime.datetime.now() - datetime.timedelta(minutes=30) # processing delay, appx

fourteen_days = datetime.timedelta(days=14)
fourteen_days_ago = datetime.datetime.now() - fourteen_days

parser.add_argument('--start', help='Start date for fetch, in ISO8601 format. Default: 14 days, 30m ago.', default=fourteen_days_ago.isoformat())
parser.add_argument('--end', help='End date for fetch, in ISO8601 format. Default: 30 min ago', default=now.isoformat())
parser.add_argument('--format', help='Record dump format. Note that the "json" formatter dumps one json object per line, rather than an array of objects, to preserve streaming', default='tsv', choices=['tsv', 'json', 'csv'])
parser.add_argument('--stardust-url', default="https://el.gc1.prod.stardust.es.net:9200")
parser.add_argument('--index', default='sd_public_interfaces')
parser.add_argument('--outfile', default=sys.stdout, type=argparse.FileType('a'))
parser.add_argument('--wide', action='store_true')
parser.add_argument('--flow', action='store_true')
parser.add_argument('--initial-count', default=0, type=int)
parser.add_argument('--username', help='Username for Elasticsearch authentication (optional)')
parser.add_argument('--password', help='Password for Elasticsearch authentication (optional)')

args = parser.parse_args()

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger()

class DataDumper:
    def __init__(self, url, verify_certs=False, request_timeout=60, username=None, password=None):
        self.url = url
        self.verify_certs = verify_certs
        self.request_timeout = request_timeout
        
        # Build Elasticsearch connection parameters
        es_params = {
            'hosts': [self.url],
            'verify_certs': self.verify_certs,
            'ssl_show_warn': False,
            'request_timeout': self.request_timeout,
        }
        
        # Add authentication if provided
        if username and password:
            es_params['basic_auth'] = (username, password)
        
        self.es = Elasticsearch(**es_params)

    def query(self, index=args.index, start=args.start, end=args.end):
        query = { "query": { 
                "range": {
                    "@timestamp": {
                        "gte": start,
                        "lte": end
                  }
                }
            },
            "sort": [{
                "@timestamp": {
                    "order": "asc"
                }
            }]
        }
        for hit in scan(self.es, index=index, query=query, preserve_order=True):
            yield hit["_source"]

    def get_fieldnames(self, wide=False, flow=False):
        if flow and wide:
            logger.error("Cannot use both --wide and --flow options together.")
            sys.exit(1)
        elif wide:
            return WIDE_FORMAT
        elif flow:
            return FLOW_FORMAT
        return NARROW_FORMAT

    def resolve(self, record, keys):
        key = keys.pop(0)
        if len(keys) > 0:
            return self.resolve(record.get(key, {}), keys)
        return record.get(key)

    def format_record(self, record, wide=False, flow=False):
        output = {}
        for compound_key in self.get_fieldnames(wide=wide, flow=flow):
            keys = compound_key.split(".")
            output[compound_key] = self.resolve(record, keys)
        return output

    def enumerate_keys(self, d, parent_key=""):
        for k in d.keys():
            key = "%s.%s" % (parent_key, k)
            yield key
            if type(d[k]) == dict:
                yield from self.enumerate_keys(d[k], parent_key=key)

    
    def dump(self, index=args.index, start=args.start, end=args.end, outfile=None, fmt=args.format, initial_count=0):
        if fmt in ["tsv", "csv"]:
            kwargs = {}
            if fmt == 'tsv':
                kwargs = { "delimiter":'\t', "lineterminator":'\n'}
            writer = csv.DictWriter(outfile, self.get_fieldnames(wide=args.wide, flow=args.flow), **kwargs)
            writer.writeheader()
            i = initial_count
            for record in self.query(index=index, start=start, end=end):
                r = self.format_record(record, wide=args.wide, flow=args.flow)
                writer.writerow(r)
                i += 1
                if (i % 10000 == 0):
                    logger.warning("Dumped 10,000 rows. Total: %s rows. last record timestamp: %s" % (i , record['@timestamp']))
        if fmt == "json":
            for record in self.query(index=index, start=start, end=end):
                r = self.format_record(record)
                json.dump(r, outfile)
                outfile.write("\n")

DataDumper(url=args.stardust_url, username=args.username, password=args.password).dump(index=args.index, start=args.start, end=args.end, outfile=args.outfile, fmt=args.format, initial_count=args.initial_count)
