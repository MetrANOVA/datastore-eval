import argparse
import sys
from time import time

from globalnoc import wsc


def get_nodes(client, start: int, end: int, base_url: str):
    client.url = f"{base_url}query.cgi"
    query = f'get node between({start}, {end}) by node from interface where node_role like "core"'

    res = client.query(query=query)
    res = res["results"]
    return [result["node"] for result in res]


def get_interfaces(client, node: str, start: int, end: int, base_url: str):
    client.url = f"{base_url}query.cgi"
    query = f'get intf between({start}, {end}) by intf, node from interface where node="{node}"'
    print(f"getting interfaces for node {node}")
    res = client.query(query=query)
    res = res["results"]

    return [result["intf"] for result in res]


def get_interface_data(
    client, node: str, intf: str, meta_fields: list, start: int, end: int, base_url: str
):
    client.url = f"{base_url}query.cgi"
    print(f"Getting interface data for {node} - {intf} from {start} to {end}")

    meta_field_str = ", ".join(meta_fields)

    query = f'get {meta_field_str}, aggregate(values.input, 60, average), aggregate(values.output, 60, average) between({start}, {end}) by intf, node from interface where(intf="{intf}" and node="{node}")'
    res = client.query(query=query)

    if int(res["total"]) > 0:
        return res
    else:
        return None


def get_metadata_fields(client, base_url: str):
    client.url = f"{base_url}metadata.cgi"
    res = client.get_meta_fields(measurement_type="interface")
    return res["results"]


def print_meta_sub_field(field: dict):
    field_keys = field.keys()

    if "name" in field_keys:
        print(f"\tname: {field["name"]}")
    if "required" in field_keys:
        print(f"\trequired: {field["required"]}")
    print()


def print_meta_field(field: dict):
    field_keys = field.keys()

    if "name" in field_keys:
        print(f"name: {field["name"]}")

    if "required" in field_keys:
        print(f"required: {field["required"]}")

    if "fields" in field_keys:
        print("Sub Fields:")
        for sub_field in field["fields"]:
            print_meta_sub_field(sub_field)

    print()


def parse_meta_sub_fields(super_field: str, fields: list):
    sub_fields = []
    for field in fields:
        sub_fields.append(f"{super_field}.{field["name"]}")
    return sub_fields


def parse_meta_fields(metafields: list):
    parsed_fields = []

    for field in metafields:
        field_keys = field.keys()
        # --- tag and kvp are currently broken in tsds and need to be stripped
        # --- out or queries will fail
        if (
            field["name"] == "tag"
            or field["name"] == "kvp"
            or field["name"] == "circuit"
            or field["name"] == "service"
        ):
            continue

        if "fields" in field_keys:
            sub_fields = parse_meta_sub_fields(field["name"], field["fields"])

            for sub_field in sub_fields:
                parsed_fields.append(sub_field)
        else:
            parsed_fields.append(f"{field["name"]}")
    return parsed_fields


def write_data(data: dict):
    return


def main(start: int, end: int, base_url: str, limit: int):
    client = wsc.WSC()
    client.strict_content_type = False

    metafields = get_metadata_fields(client, base_url)
    parsed_meta_fields = parse_meta_fields(metafields)

    data = {}
    total = 0

    nodes = get_nodes(client, start, end, base_url)
    for node in nodes:
        if limit != 0 and total >= limit:
            break

        interfaces = get_interfaces(client, node, start, end, base_url)
        data[node] = {}

        for interface in interfaces:
            if limit != 0 and total >= limit:
                break

            interface_data = get_interface_data(
                client, node, interface, parsed_meta_fields, start, end, base_url
            )

            if interface_data:
                data[node][interface] = interface_data
                total = total + 1
            else:
                print(f"Skipping {node}-{interface} because no data was returned")

    write_data(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="get_data arguments")

    # Optional arguments
    parser.add_argument(
        "--start",
        type=int,
        default=int(time() - 3600),
        help="Start time as a UNIX Epoch",
    )
    parser.add_argument(
        "--end",
        type=int,
        default=int(time()),
        help="End time as a UNIX Epoch",
    )
    parser.add_argument(
        "--url",
        type=str,
        default="https://services.tsds.bldc.net.internet2.edu/i2/services/",
        help="TSDS URL to fetch data from",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="The number of node-intf pairs to pull data for.",
    )

    args = parser.parse_args()

    main(args.start, args.end, args.url, args.limit)
