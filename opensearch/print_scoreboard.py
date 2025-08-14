import argparse
from opensearchpy import OpenSearch

parser = argparse.ArgumentParser(description='Prints scoreboard information after inserts into opensearch.')

parser.add_argument('--user', help='Opensearch username', default='timescale')
parser.add_argument('--password', help='Opensearch User Password', default='timescale')
parser.add_argument('--host', help="remote opensearch host", default='localhost')
parser.add_argument('--port', help="remote opensearch port", default='9200')

parser.add_argument('--scoreboard-index', help="Table to insert metrics/scoreboard info.", default='scoreboard')

parser.add_argument('--batch-size', help="Specify the insert batch size", type=int, default=25000)

arguments = parser.parse_args()

os_client = OpenSearch(hosts=[{"host": arguments.host, "port": arguments.port}], basic_auth=(arguments.user, arguments.password))

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

res = os_client.search(index=arguments.scoreboard_index, body=doc)

for bucket in res['aggregations']['insertions']['buckets']:
    print("%s: %s inserts per second" % (bucket['key_as_string'], (bucket['doc_count'] * arguments.batch_size) / 60))
