CREATE TABLE Candles (
    asset STRING,
    st TIMESTAMP(9),
    et TIMESTAMP(9),
    `O` DOUBLE,
    `C` DOUBLE,
    `H` DOUBLE,
    `L` DOUBLE,
    `V` DOUBLE,
    `TA` DOUBLE
) WITH (
 'connector' = 'filesystem',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'ISO-8601'
);