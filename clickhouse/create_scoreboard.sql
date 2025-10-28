CREATE TABLE IF NOT EXISTS `datastoreEval`.`scoreboard`
(
   table_name String,
   batch_size Float64,
   start_time DateTime64(3, 'UTC'),
   end_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(`start_time`)
ORDER BY (`start_time`);
