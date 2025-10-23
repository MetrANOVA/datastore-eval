-- 2. │ flow_edge_5m_asns_v2       │
-- 3. │ flow_edge_5m_asns_v2_mv    │
-- 4. │ flow_edge_5m_asns_v3       │
-- 5. │ flow_edge_5m_if            │
-- 6. │ flow_edge_5m_if_mv         │
-- 7. │ flow_edge_5m_ip_version    │
-- 8. │ flow_edge_5m_ip_version_mv │
-- 9. │ flow_edge_v2               │


CREATE TABLE IF NOT EXISTS `metranova`.`flow_edge_v2_tee` AS `metranova`.`flow_edge_v2`;

INSERT INTO `metranova`.`administration_events` (event, operator, query) VALUES ('create flow tee target table', 'jkafader', 'CREATE TABLE `metranova`.`flow_edge_v2_tee` AS `metranova`.`flow_edge_v2`');

CREATE MATERIALIZED VIEW IF NOT EXISTS `metranova`.`flow_edge_v2_tee_mv` TO `metranova`.`flow_edge_v2_tee` ENGINE=MergeTree() AS SELECT * FROM `metranova`.`flow_edge_v2`;

INSERT INTO `metranova`.`administration_events` (event, operator, query) VALUES ('create flow tee materialized view', 'jkafader', 'CREATE MATERIALIZED VIEW IF NOT EXISTS `metranova`.`flow_edge_v2_tee_mv` TO `metranova`.`flow_edge_v2_tee` AS SELECT * FROM `metranova`.`flow_edge_v2`;');


ALTER TABLE `metranova`.`flow_edge_v2_tee` MODIFY SETTINGS storage_policy='hot_warm_cold';

-- add TTLs. New data goes to hot. 2 days old goes to warm. 4 days old goes to cold. 6 days old delete.
ALTER TABLE `metranova`.`flow_edge_v2_tee` MODIFY TTL
    insert_time TO VOLUME 'hot_volume',
    insert_time + INTERVAL 2 DAY TO VOLUME 'warm_volume',
    insert_time + INTERVAL 4 DAY TO VOLUME 'cold_volume',
    insert_time + INTERVAL 6 DAY DELETE; 

INSERT INTO `metranova`.`administration_events` (event, operator, query) VALUES ('add TTLs to flow tee table', 'jkafader', 'ALTER TABLE `metranova`.`flow_edge_v2_tee` MODIFY TTL insert_time TO VOLUME hot_volume, insert_time + INTERVAL 2 DAY TO VOLUME warm_volume,	insert_time + INTERVAL 4 DAY TO VOLUME cold_volume, insert_time + INTERVAL 6 DAY DELETE;');

INSERT INTO `metranova`.`flow_edge_v2_tee` SELECT * from `metranova`.`flow_edge_v2`;

INSERT INTO `metranova`.`administration_events` (event, operator, query) VALUES ('copy old data from flow_edge_v2 to flow_edge_v2_tee', 'jkafader', 'INSERT INTO `metranova`.`flow_edge_v2_tee` SELECT * from `metranova`.`flow_edge_v2`;')


ALTER TABLE `metranova`.`flow_edge_v2_tee` MODIFY SETTINGS ttl_only_drop_parts = 1

INSERT INTO `metranova`.`administration_events` (event, operator, query) VALUES ('copy old data from flow_edge_v2 to flow_edge_v2_tee', 'jkafader', 'ALTER TABLE `metranova`.`flow_edge_v2_tee` MODIFY SETTINGS ttl_only_drop_parts = 1;')
