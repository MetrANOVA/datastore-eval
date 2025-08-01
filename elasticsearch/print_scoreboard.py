import argparse
from elasticsearch import Elasticsearch

parser = argparse.ArgumentParser(description='Inserts ESnet Stardust Data into elasticsearch.')

parser.add_argument('--user', help='Elasticsearch username', default='timescale')
parser.add_argument('--password', help='Elasticsearch User Password', default='timescale')
parser.add_argument('--host', help="remote elastic host", default='localhost')
parser.add_argument('--port', help="remote elastic port", default='9200')

parser.add_argument('--scoreboard-index', help="Table to insert metrics/scoreboard info.", default='scoreboard')

parser.add_argument('--batch-size', help="Specify the insert batch size", type=int, default=25000)

arguments = parser.parse_args()

es_client = Elasticsearch(hosts=["http://%s:%s" % (arguments.host, arguments.port)], basic_auth=(arguments.user, arguments.password))

doc = {
        'size' : 0,
        'aggs': {
            'insertions' : {
                'date_histogram': {
                    'field': 'start_time',
                    'calendar_interval': "1m", 
                }
            }
       }
   }

res = es_client.search(index=arguments.scoreboard_index, body=doc)

for bucket in res['aggregations']['insertions']['buckets']:
    print("%s: %s inserts per second" % (bucket['key_as_string'], (bucket['doc_count'] * arguments.batch_size) / 60))
