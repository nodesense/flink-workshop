CREATE TABLE DataValues (
  asset STRING,
  name  STRING,
  tag STRING,
  ts TIMESTAMP(3),
  `value` DOUBLE

) WITH (
 'connector' = 'filesystem',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true',
 'path' = '/home/krish/IdeaProjects/FlinkDemo/data/ticks-mini.json',
 'json.timestamp-format.standard' = 'ISO-8601'
);