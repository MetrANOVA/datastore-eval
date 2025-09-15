from mappings import WIDE_FORMAT, NARROW_FORMAT, FLOW_FORMAT
from collections import defaultdict
import orjson
import datetime

def set_element(d, element, newval):
    keys = element.split('.')
    last_key = keys.pop()
    to_set = d
    for key in keys:
        if not key in to_set or not type(to_set[key]) == dict:
            to_set[key] = dict()
        to_set = to_set[key]
    to_set[last_key] = newval

def assemble_standard(row, header, fmt, original_line):
    values = []
    metadata = {}
    for key, val in fmt.items():
        column_offset = header.index(key)
        if not key.startswith('meta'):
            if key in ["@timestamp","@exit_time","@collect_time_min"]:
                values.append(datetime.datetime.fromisoformat(row[column_offset].replace("Z", "+00:00")))
            else:
                try:
                    value = float(row[column_offset])
                except ValueError:
                    value = None
                values.append(value)
        if key.startswith("meta"):
            element = val
            if row[column_offset] == "":
                newval = None
            else:
                newval = row[column_offset]
            set_element(metadata, element, newval)
    values.append(orjson.dumps(metadata))
    return values
    
def assemble_wide_normalized(row, header, fmt, original_line):
    values = []
    queues = {}
    for key, val in fmt.items():
        column_offset = header.index(key)
        if key.startswith("values.queue"):
            element = val
            newval = row[column_offset]
            set_element(queues, element, newval)
        else:
            if key in ["@timestamp","@exit_time","@collect_time_min"]:
                value = datetime.datetime.fromisoformat(row[column_offset].replace("Z", "+00:00"))
            if key.startswith("meta"):
                if row[column_offset] == "":
                    value = None
                else:
                    value = row[column_offset]
            if key.startswith("value"):
                try:
                    value = float(row[column_offset])
                except ValueError:
                    value = None
            values.append(value)
    values.append(orjson.dumps(queues))
    return values

def assemble_flow(row, header, fmt, original_line):
    objects = {
        "bgp": {},
        "esdb": {},
        "mpls": {},
        "scireg": {},
    }
    values = []
    for key, val in fmt.items():
        column_offset = header.index(key)
        # if val startswith bgp, esdb, mpls, or scireg, set a key on the appropriate object
        for name, target in objects.items():
            if val.startswith(name):
                element = val.replace("%s." % name, "")
                if row[column_offset] == "":
                    value = None
                else:
                    value = row[column_offset]
                set_element(target, element, value)
        if val in ["_timestamp","_exit_time","_collect_time_min", "_start", "_end"]:
            value = datetime.datetime.fromisoformat(row[column_offset].replace("Z", "+00:00"))
            values.append(value)
        if val.startswith("meta") or val.startswith("value") or val in ["_processing_time", "stitched_flows", "ts_id", "type"]:
            if val.startswith("meta") or val in ["stitched_flows", "ts_id", "type"]:
                if row[column_offset] == "":
                    value = None
                if row[column_offset] in ["True", "False"]:
                    value = bool(row[column_offset])
                else:
                    value = row[column_offset]
            else:
                try:
                    value = float(row[column_offset])
                except ValueError:
                    value = None
            values.append(value)
    values.append(orjson.dumps(objects["bgp"]))
    values.append(orjson.dumps(objects["esdb"]))
    values.append(orjson.dumps(objects["mpls"]))
    values.append(orjson.dumps(objects["scireg"]))
    return values
    
def assemble(row, header, fmt=NARROW_FORMAT, original_line=None, normalized=False, flow=False):
    if normalized and not flow:
        # last column is 'queues'
        return assemble_wide_normalized(row, header, fmt, original_line)
    if flow and not normalized:
        # last 4 columns are 'bgp', 'esdb', 'mpls', and 'scireg'
        return assemble_flow(row, header, fmt, original_line)
    if not flow and not normalized:
        # last column is 'metadata'
        return assemble_standard(row, header, fmt, original_line)

    
