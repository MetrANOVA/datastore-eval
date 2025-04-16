from mappings import STARDUST_NARROW, STARDUST_WIDE
from collections import defaultdict

def set_element(d, element, newval):
    keys = element.split('.')
    last_key = keys.pop()
    to_set = d
    for key in keys:
        if not key in to_set or not type(to_set[key]) == dict:
            to_set[key] = dict()
        to_set = to_set[key]
    to_set[last_key] = newval

def assemble(row, header, fmt=STARDUST_NARROW):
    values = []
    metadata = {}
    for key, val in fmt.keys():
        column_offset = header.index(key)
        if key.startswith("values"):
            values.push(row[column_offset])
        if key.startswith("meta"):
            element = val
            newval = row[column_offset]
            set_element(metadata, element, newval)
    return values, metadata
    
