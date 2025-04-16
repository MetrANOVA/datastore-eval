CREATE DATABASE IF NOT EXISTS metranova;

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE IF NOT EXISTS metranova.flow (
    import_time_ms DateTime64(9, 'UTC'),
    start_time_ms DateTime64(9, 'UTC'),
    end_time_ms DateTime64(9, 'UTC'),
    router_ip IPv6,
    router_name LowCardinality(String),
    interface_in_id UInt16,
    interface_in_name LowCardinality(String),
    interface_out_id UInt16,
    interface_out_name LowCardinality(String),
    ip_version UInt8,
    vlan_id UInt16,
    asn_src LowCardinality(UInt32),
    asn_dst LowCardinality(UInt32),
    hash_fwd UInt64,
    hash_rev UInt64,
    ip_src String,
    ip_src_bin IPv6,
    ip_dst String,
    ip_dst_bin IPv6,
    ip_proto LowCardinality(String),
    src_port UInt16,
    dst_port UInt16,
    vrf_in UInt8,
    vrf_out UInt8,
    packets UInt64,
    bytes UInt64,
    mpls_labels Array(Nullable(UInt32)),
) ENGINE = ReplacingMergeTree()
  PARTITION BY date_trunc('hour', start_time_ms)
  PRIMARY KEY (start_time_ms, router_name, interface_in_name, interface_out_name, ip_src_bin, ip_dst_bin)
  ORDER BY (start_time_ms, router_name, interface_in_name, interface_out_name, ip_src_bin, ip_dst_bin, ip_proto)
  TTL toDateTime(start_time_ms) + INTERVAL 7 DAY DELETE
  SETTINGS ttl_only_drop_parts = 1;


CREATE VIEW IF NOT EXISTS metranova.flow_enriched
AS
   select *,
    dictGet('metranova.ip2location', 'city_name', ip_src_bin) AS city_src,
    dictGet('metranova.ip2location', 'country_name', ip_src_bin) AS country_src,
    dictGet('metranova.ip2location', 'continent_name', ip_src_bin) AS continent_src,
    dictGet('metranova.ip2location', 'location_latitude', ip_src_bin) AS latitude_src,
    dictGet('metranova.ip2location', 'location_longitude', ip_src_bin) AS longitude_src,

    dictGet('metranova.ip2location', 'city_name', ip_dst_bin) AS city_dst,
    dictGet('metranova.ip2location', 'country_name', ip_dst_bin) AS country_dst,
    dictGet('metranova.ip2location', 'continent_name', ip_dst_bin) AS continent_dst,
    dictGet('metranova.ip2location', 'location_latitude', ip_dst_bin) AS latitude_dst,
    dictGet('metranova.ip2location', 'location_longitude', ip_dst_bin) AS longitude_dst,

    dictGet('metranova.ip2as', 'autonomous_system_organization', ip_src_bin) AS organization_src,
    dictGet('metranova.ip2as', 'autonomous_system_organization', ip_dst_bin) AS organization_dst
FROM metranova.flow; 


CREATE TABLE IF NOT EXISTS metranova.stardust_flow_kafka_queue (
       message String
) ENGINE = Kafka(stardust_kafka);

-- convert raw labels from pmacct to ints - comes as hex with bytes divided by -.
-- Example: 7F-FB-C1
-- See https://tools.ietf.org/html/rfc5462 for label structure
-- - First 20 bits as decimal are what we want
-- - next 3 bits are experimental
-- - last bit is S bit
DROP FUNCTION IF EXISTS mpls_string_to_int;
CREATE FUNCTION mpls_string_to_int AS (hex_string) -> 
            if(empty(hex_string), null,
               bitShiftRight(
                reinterpretAsInt64(
                    reverse(
                        unhex(
                            replaceAll(hex_string, '-', '')
                        )
                    )
                ), 4)
            );
            

CREATE MATERIALIZED VIEW IF NOT EXISTS metranova.stardust_flow_materialized_view TO metranova.flow AS
SELECT
    JSONExtract(message, 'ip_src', 'String') as ip_src,
    JSONExtract(message, 'ip_dst', 'String') as ip_dst,
    toIPv6(ip_src) as ip_src_bin,
    toIPv6(ip_dst) as ip_dst_bin,
    JSONExtract(message, 'as_src', 'UInt32') as asn_src,
    JSONExtract(message, 'as_dst', 'UInt32') as asn_dst,
    JSONExtract(message, 'packets', 'UInt64') AS packets,
    JSONExtract(message, 'bytes', 'UInt64') AS bytes,

    fromUnixTimestamp64Milli(JSONExtract(message, 'flow_start_ms', 'UInt64')) AS start_time_ms,
    fromUnixTimestamp64Milli(JSONExtract(message, 'flow_end_ms', 'UInt64'))  AS end_time_ms,
    now64(3, 'UTC') AS import_time_ms,

    JSONExtract(message, 'port_src', 'UInt16') AS src_port,
    JSONExtract(message, 'port_dst', 'UInt16') AS dst_port,
    JSONExtract(message, 'ip_proto', 'String') AS ip_proto,

    toIPv6(JSONExtract(message, 'peer_ip_src', 'String')) as router_ip,
    dictGet('metranova.routers', 'name', router_ip) AS router_name,
    
    JSONExtract(message, 'iface_in', 'UInt32') AS interface_in_id,
    JSONExtract(message, 'iface_out', 'UInt32') AS interface_out_id,
    dictGet('metranova.router_interfaces', 'if_name', (router_name, interface_in_id)) AS interface_in_name,
    dictGet('metranova.router_interfaces', 'if_name', (router_name, interface_out_id)) AS interface_out_name,

    JSONExtract(message, 'vlan_id', 'UInt16') AS vlan_id,
    JSONExtract(message, 'vrfid_ingress', 'UInt8') AS vrf_in,
    JSONExtract(message, 'vrfid_egress', 'UInt8') AS vrf_out,

    if(isIPv4String(ip_src), 4, 6) as ip_version,

    arrayMap(x -> mpls_string_to_int(x),
    [
        JSONExtract(message, 'mpls_label1', 'String'),
        JSONExtract(message, 'mpls_label2', 'String'),
        JSONExtract(message, 'mpls_label3', 'String'),
        JSONExtract(message, 'mpls_label4', 'String'),
        JSONExtract(message, 'mpls_label5', 'String'),
        JSONExtract(message, 'mpls_label6', 'String'),
        JSONExtract(message, 'mpls_label7', 'String'),
        JSONExtract(message, 'mpls_label8', 'String'),
        JSONExtract(message, 'mpls_label9', 'String'),
        JSONExtract(message, 'mpls_label10', 'String')
    ]) as mpls_labels,

    farmFingerprint64(ip_src_bin, ip_dst_bin, ip_proto, src_port, dst_port) AS hash_fwd,
    farmFingerprint64(ip_dst_bin, ip_src_bin, ip_proto, dst_port, src_port) AS hash_rev

FROM metranova.stardust_flow_kafka_queue;