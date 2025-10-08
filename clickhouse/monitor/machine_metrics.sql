CREATE TABLE IF NOT EXISTS `datastoreEval`.`snmp_data` (
    `cpu_idle_percentage` Nullable(Float64),
    `load_avg` Nullable(Float64),
    `memory_pressure` Nullable(Float64),
    `io_pressure` Nullable(Float64),
    `cpu_pressure` Nullable(Float64),
    `insert_time` DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (`meta.name`, `meta.device`, `@timestamp`);
