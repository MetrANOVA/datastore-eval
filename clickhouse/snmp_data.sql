-- Generated Schema from stardust_data-2025-03-28--2025-03-29.tsv
-- Target Database: datastoreEval
-- Target Table: snmp_data

CREATE TABLE IF NOT EXISTS `datastoreEval`.`snmp_data` (
    `values.if_in_bits.delta` Nullable(Float64),
    `values.if_out_bits.delta` Nullable(Float64),
    `values.in_bcast_pkts.delta` Nullable(Float64),
    `values.out_bcast_pkts.delta` Nullable(Float64),
    `values.in_bits.delta` Nullable(Float64),
    `values.out_bits.delta` Nullable(Float64),
    `values.in_discards.delta` Nullable(Float64),
    `values.out_discards.delta` Nullable(Float64),
    `values.in_errors.delta` Nullable(Float64),
    `values.out_errors.delta` Nullable(Float64),
    `values.in_mcast_pkts.delta` Nullable(Float64),
    `values.out_mcast_pkts.delta` Nullable(Float64),
    `values.in_ucast_pkts.delta` Nullable(Float64),
    `values.out_ucast_pkts.delta` Nullable(Float64),
    `@timestamp` DateTime64(3, 'UTC'),
    `@processing_time` Nullable(String),
    `@exit_time` Nullable(String),
    `@collect_time_min` Nullable(String),
    `meta.if_oper_status` Nullable(String),
    `meta.device` String,
    `meta.if_admin_status` Nullable(String),
    `meta.oper_status` Nullable(String),
    `meta.descr` Nullable(String),
    `meta.speed` Nullable(String),
    `meta.alias` Nullable(String),
    `meta.device_info.state` Nullable(String),
    `meta.device_info.os` Nullable(String),
    `meta.device_info.loc_name` Nullable(String),
    `meta.device_info.loc_type` Nullable(String),
    `meta.device_info.location.lat` Nullable(String),
    `meta.device_info.location.lon` Nullable(String),
    `meta.device_info.network` Nullable(String),
    `meta.device_info.role` Nullable(String),
    `meta.admin_status` Nullable(String),
    `meta.name` String,
    `meta.intercloud` Nullable(String),
    `meta.port_mode` Nullable(String),
    `meta.ifindex` Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(`@timestamp`)
ORDER BY (`meta.name`, `meta.device`, `@timestamp`);
