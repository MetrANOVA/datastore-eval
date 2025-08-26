-- Generated Schema from stardust_data-2025-03-28--2025-03-29_reversed.tsv
-- Target Database: datastoreEval
-- Target Table: snmp_data

CREATE TABLE IF NOT EXISTS `datastoreEval`.`snmp_data` (
    `if_in_bits.delta` Nullable(Float64),
    `if_out_bits.delta` Nullable(Float64),
    `in_bcast_pkts.delta` Nullable(Float64),
    `out_bcast_pkts.delta` Nullable(Float64),
    `in_bits.delta` Nullable(Float64),
    `out_bits.delta` Nullable(Float64),
    `in_discards.delta` Nullable(Float64),
    `out_discards.delta` Nullable(Float64),
    `in_errors.delta` Nullable(Float64),
    `out_errors.delta` Nullable(Float64),
    `in_mcast_pkts.delta` Nullable(Float64),
    `out_mcast_pkts.delta` Nullable(Float64),
    `in_ucast_pkts.delta` Nullable(Float64),
    `out_ucast_pkts.delta` Nullable(Float64),
    `timestamp` DateTime64(3, 'UTC'),
    `if_oper_status` Nullable(String),
    `device` String,
    `if_admin_status` Nullable(String),
    `oper_status` Nullable(String),
    `descr` Nullable(String),
    `speed` Nullable(String),
    `alias` Nullable(String),
    `device_info.state` Nullable(String),
    `device_info.os` Nullable(String),
    `device_info.loc_name` Nullable(String),
    `device_info.loc_type` Nullable(String),
    `device_info.location.lat` Nullable(String),
    `device_info.location.lon` Nullable(String),
    `device_info.network` Nullable(String),
    `device_info.role` Nullable(String),
    `admin_status` Nullable(String),
    `interfaceName` String,
    `intercloud` Nullable(String),
    `port_mode` Nullable(String),
    `ifindex` Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(`timestamp`)
ORDER BY (`device`, `interfaceName`, `timestamp`);
