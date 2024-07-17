# metranova-clickhouse-testing

This repository contains a semi complete testing environment for ingesting flow data into Clickhouse, based on running a Docker container.

The current setup depends on flow data records being exporterd from pmacct/nfacctd into a kafka queue. Clickhouse then connects to this kafka queue and pulls the records periodically.

In addition to the raw data, this setup also includes some examples of metadata enrichment in the dictionaries subdirectory. This hopefully showcases how the raw netflow records can be enriched to provide a better human readable experience, such as translating IPs into hostnames, ifindexes into interface names, and IP address metadata such as location.

# To use

Caveats - you will need to have the pmacct/nfacctd exporting to a kafka queue already for this to connect to. A better example in the future might have a reference dataset to ingest. You can at least get the server up and running without this, but it will log errors about being unable to connect to kafka and naturally there will be no data to look at.

`docker-compose up`

This will create a Clickhouse server container running listening on port 9000. For simplicity there is no auth or encryption setup here at the moment, though Clickhouse does support that.

You will need the clickhouse client installed to connect. See https://clickhouse.com/docs/en/install

Once installed, load the SQL files.

`clickhouse client --queries-file sql/lookup_tables.sql`
`clickhouse client --queries-file sql/stardust_flow.sql`


To take a look around:

`clickhouse client`
`use metranova;`
`show tables;`

```
bb72cb7323a0 :) show tables;

SHOW TABLES

Query id: c56fa23a-1122-45d9-90be-9a9ac0b35afd

   ┌─name────────────────────────────┐
1. │ flow                            │
2. │ flow_enriched                   │
3. │ ip2as                           │
4. │ ip2location                     │
5. │ router_interfaces               │
6. │ routers                         │
7. │ stardust_flow_kafka_queue       │
8. │ stardust_flow_materialized_view │
   └─────────────────────────────────┘
```

See the sql files for more information about how the tables are actually created.