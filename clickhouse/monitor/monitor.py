import subprocess
import os
import re
import argparse
import clickhouse_connect
import logging
import sys

parser = argparse.ArgumentParser(prog='Periodic Monitoring Script',
                    description='Monitors key system pressure statistics. Intended to be run once per minute')

parser.add_argument("--host", default="localhost")
parser.add_argument("--database", default='datastoreEval')
parser.add_argument("--port", default="8123")
parser.add_argument("--user", default="default")
parser.add_argument("--password", default='unknown')
parser.add_argument("--log-level", default='info')

arguments = parser.parse_args()

MEASUREMENTS = {
    "cpu_idle": {"command": ["bash", "-c", "mpstat 1 1 | grep Average | awk '{print $12}'"], "parser": rb"([\d\.]+)" },
    "load_avg": { "command": ["uptime"], "parser": rb'.*load average: ([\d\.]+)' },
    "memory_pressure": { "command": ["cat", "/proc/pressure/memory"], "parser": rb'some.*avg60=([\d\.]+)' },
    "io_pressure": { "command": ["cat", "/proc/pressure/io"], "parser": rb'some.*avg60=([\d\.]+)' },
    "cpu_pressure": { "command": ["cat", "/proc/pressure/cpu"], "parser": rb'some.*avg60=([\d\.]+) '} ,
}

logging.basicConfig(level = getattr(logging, arguments.log_level.upper()),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

def do_measurements():
    output = {}
    for column, measurement in MEASUREMENTS.items():
        cmd = measurement['command']
        p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)

        for line in p.stdout:
            parser = re.compile(measurement['parser'])
            match = parser.match(line)
            if match:
                # match.groups(0) is e.g. (b'0.00',)
                output[column] = float(match.groups(0)[0])
    keys = [
        'cpu_idle',
        'load_avg',
        'memory_pressure',
        'io_pressure',
        'cpu_pressure',
    ]
    return [output[k] for k in keys]

def maybe_create_table(client):
    file_path = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(file_path, "machine_metrics.sql")) as infile:
        create_statement = infile.read()
        client.command(create_statement)

def insert_measurements():
    
    try:
        client = clickhouse_connect.get_client(
            host=arguments.host, port=arguments.port, user=arguments.user,
            password=arguments.password, database=arguments.database
        )
        client.ping()
        logger.info(f"Connected to DB '{db_name}', targeting Table '{table_name}'.")
    except Exception as e:
        logger.error(f"Error connecting to ClickHouse: {e}")
        sys.exit(1)

    maybe_create_table(client)
    measurements = do_measurements()
    client.insert('machine_measurements', measurements)

insert_measurements()
