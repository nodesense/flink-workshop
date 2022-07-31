CREATE TABLE Rainbows (
    asset STRING,
    ts TIMESTAMP(9),
    `wave1` DOUBLE,
    `wave2` DOUBLE,
    `wave3` DOUBLE,
    `wave4` DOUBLE,
    `wave5` DOUBLE,
    `wave6` DOUBLE,
    `wave7` DOUBLE,
    `wave8` DOUBLE,
    `wave9` DOUBLE,
    `wave10` DOUBLE,
    `wave11` DOUBLE,
    `wave12` DOUBLE,
    `wave13` DOUBLE,
    `wave14` DOUBLE,
    `wave15` DOUBLE,
    `wave16` DOUBLE,
    `wave17` DOUBLE,
    `wave18` DOUBLE,
    `wave19` DOUBLE,
    `wave20` DOUBLE,
    `wave21` DOUBLE,
    `wave22` DOUBLE,
    `wave23` DOUBLE,
    `wave24` DOUBLE,
    `adiff` DOUBLE,
    `sdiff` DOUBLE,
    `signal` DOUBLE,
    `williamr` DOUBLE,
     `cci` DOUBLE,
     `rsi` DOUBLE,
     `kcm` DOUBLE,
    `kcu` DOUBLE,
    `kcl` DOUBLE,
    `roc` DOUBLE,

    `ltp` DOUBLE,
    `arup` DOUBLE,
    `ardown` DOUBLE,
    `aroc` DOUBLE,
    `lreg` DOUBLE
) WITH (
 'connector' = 'filesystem',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true',
 'path' = 'file:///media/krish/IntelNVMe/truedata/flink-json-results',
 'json.timestamp-format.standard' = 'ISO-8601'
);