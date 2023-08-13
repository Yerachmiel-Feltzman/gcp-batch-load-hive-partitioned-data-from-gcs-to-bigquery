CREATE TABLE IF NOT EXISTS
    `my-project.prod_landing_zone.load_per_hourly_partition`
(
 <SCHEMA>
)
PARTITION BY
    TIMESTAMP_TRUNC(<PARTITION_FIELD>, HOUR)
CLUSTER BY
    <CLUSTER_FIELD>
OPTIONS (
--    partition_expiration_days = 7,
    require_partition_filter = TRUE
 );
