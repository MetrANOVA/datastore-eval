CREATE TABLE IF NOT EXISTS `metranova`.`administration_events` (
    `event` String, -- name of event. not null.
    `query` Nullable(String), -- query. optional.
    `operator` Nullable(String), -- The user actually doing the event. optional.
    `insert_time` DateTime64(3, 'UTC') DEFAULT now() -- timestamp
)
ENGINE = MergeTree()
ORDER BY (`insert_time`);
