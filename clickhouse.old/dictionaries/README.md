# Dictionaries

These files exist to provide examples of how metadata enrichment can occur within Clickhouse. The structure is important but the contents are effectively placeholders. These could be generated in a number of different ways.

### ip2as
This maps IP ranges into AS information, specifically the organization information about that AS.

### ip2location
This maps IP ranges into locatino based information, such as lat/lon, city, country, etc.

### router_interfaces
This provides a mapping for translating the numeric ifindex in netflow records into interface names. It is a compound key of router name + ifindex.

### router_ips
This provides a mapping for translating netflow's router IP address into a human friendly router name.