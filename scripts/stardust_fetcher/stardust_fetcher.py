import argparse
import csv
import json
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import sys

parser = argparse.ArgumentParser(description='Fetches public data from ESnet Stardust, formatting the output as csv, tsv, or json.')

now = datetime.datetime.now() - datetime.timedelta(minutes=30) # processing delay, appx

fourteen_days = datetime.timedelta(days=14)
fourteen_days_ago = datetime.datetime.now() - fourteen_days

parser.add_argument('--start', help='Start date for fetch, in ISO8601 format. Default: 90 days ago.', default=fourteen_days_ago.isoformat())
parser.add_argument('--end', help='End date for fetch, in ISO8601 format. Default: today', default=now.isoformat())
parser.add_argument('--format', help='Record dump format. Note that the "json" formatter dumps one json object per line, rather than an array of objects, to preserve streaming', default='tsv', choices=['tsv', 'json', 'csv'])
parser.add_argument('--stardust-url', default="https://el.gc1.prod.stardust.es.net:9200")
parser.add_argument('--index', default='sd_public_interfaces')
parser.add_argument('--outfile', default=sys.stdout)

args = parser.parse_args()

class DataDumper:
    def __init__(self, url, verify_certs=False, request_timeout=60):
        self.url = url
        self.verify_certs = verify_certs
        self.request_timeout = request_timeout
        self.es = Elasticsearch(
                    self.url,
                    verify_certs=self.verify_certs,
                    ssl_show_warn=False,
                    request_timeout=self.request_timeout,
                )

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
                    "order": "desc"
                }
            }]
        }
        for hit in scan(self.es, index=index, query=query, preserve_order=True):
            yield hit["_source"]

    def get_fieldnames(self):
        return [
            'values.if_in_bits.delta',
            'values.if_out_bits.delta',
            'values.in_bcast_pkts.delta',
            'values.out_bcast_pkts.delta',
            'values.in_bits.delta',
            'values.out_bits.delta',
            'values.in_discards.delta',
            'values.out_discards.delta',
            'values.in_errors.delta',
            'values.out_errors.delta',
            'values.in_mcast_pkts.delta',
            'values.out_mcast_pkts.delta',
            'values.in_ucast_pkts.delta',
            'values.out_ucast_pkts.delta',
            '@timestamp',
            '@processing_time',
            '@exit_time',
            '@collect_time_min',
            'meta.if_oper_status',
            'meta.device',
            'meta.if_admin_status',
            'meta.oper_status',
            'meta.descr',
            'meta.speed',
            'meta.alias',
            'meta.device_info.state',
            'meta.device_info.os',
            'meta.device_info.loc_name',
            'meta.device_info.loc_type',
            'meta.device_info.location.lat',
            'meta.device_info.location.lon',
            'meta.device_info.network',
            'meta.device_info.role',
            'meta.admin_status',
            'meta.name',
            'meta.intercloud',
            'meta.port_mode',
            'meta.ifindex'
        ]

    def resolve(self, record, keys):
        key = keys.pop(0)
        if len(keys) > 0:
            return self.resolve(record.get(key, {}), keys)
        return record.get(key)

    def format_record(self, record):
        output = {}
        for compound_key in self.get_fieldnames():
            keys = compound_key.split(".")
            output[compound_key] = self.resolve(record, keys)
        return output

    def dump(self, index=args.index, start=args.start, end=args.end, outfile=args.outfile, fmt=args.format):
        if fmt in ["tsv", "csv"]:
            kwargs = {}
            if format == 'tsv':
                kwargs = { "delimiter":'\t', "lineterminator":'\n'}
            writer = csv.DictWriter(outfile, self.get_fieldnames(), **kwargs)
            writer.writeheader()
            for record in self.query(index=index, start=start, end=end):
                r = self.format_record(record)
                writer.writerow(r)
        if fmt == "json":
            for record in self.query(index=index, start=start, end=end):
                r = self.format_record(record)
                json.dump(r, outfile)
                outfile.write("\n")


DataDumper(url=args.stardust_url).dump(index=args.index, start=args.start, end=args.end, fmt=args.format)
