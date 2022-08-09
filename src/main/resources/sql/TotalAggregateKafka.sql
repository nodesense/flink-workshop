CREATE TABLE TradeAggregates (
  asset STRING,
  st TIMESTAMP(9),
  et TIMESTAMP(9),
  DT  STRING,
  V DOUBLE,
  TA  DOUBLE,
  OI DOUBLE,
  OIDiff DOUBLE,
  PRIMARY KEY (DT, st, et) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'stock-aggregates2',
  'properties.bootstrap.servers' = 'broker:9092',
  'value.format' = 'json',
  'key.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);
