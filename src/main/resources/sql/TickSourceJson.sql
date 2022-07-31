CREATE TABLE Ticks (
    asset STRING,
    ts TIMESTAMP_LTZ,
    LTP DOUBLE,
    ATP DOUBLE,
    Vol DOUBLE,
    AP DOUBLE,
    BP DOUBLE,
    AQ DOUBLE,
    BQ DOUBLE,
    OI DOUBLE,
    DTO DOUBLE
) WITH (
 'connector' = 'filesystem',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true',
 'path' = 'file:///media/krish/IntelNVMe/truedata/flink-json/2022-07-18.json',
 'json.timestamp-format.standard' = 'ISO-8601'
);