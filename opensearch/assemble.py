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

def assemble(row, header, fmt=NARROW_FORMAT, original_line=None, no_datastream=False):
    document = {}
    for key, val in fmt.items():
        column_offset = header.index(key)
        try:
            element = val
            newval = row[column_offset]
            set_element(document, element, newval)
        except:
            import pdb; pdb.set_trace()

    if no_datastream:
        return document

    return { "_op_type": "create", "_source": document }

