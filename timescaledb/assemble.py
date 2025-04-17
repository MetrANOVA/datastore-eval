from mappings import WIDE_FORMAT, NARROW_FORMAT
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

def assemble(row, header, fmt=NARROW_FORMAT, original_line=None):
    values = []
    metadata = {}
    for key, val in fmt.items():
        column_offset = header.index(key)
        if not key.startswith("meta"):
            try:
                if key in ["@timestamp","@exit_time","@collect_time_min"]:
                    values.append(datetime.datetime.fromisoformat(row[column_offset].replace("Z", "+00:00")))
                else:
                    values.append(float(row[column_offset]) if row[column_offset] else None)
            except:
                import pdb; pdb.set_trace()
        if key.startswith("meta"):
            try:
                element = val
                newval = row[column_offset]
                set_element(metadata, element, newval)
            except:
                import pdb; pdb.set_trace()
    # last column is always metadata as a JSON blob
    values.append(orjson.dumps(metadata))
    return values
    
