CREATE TABLE CandleKafka (
  asset STRING,
  st TIMESTAMP(9),
  et TIMESTAMP(9),
    O DOUBLE,
    C DOUBLE,
    H DOUBLE,
    L DOUBLE,
    A DOUBLE,

  DT  STRING,
  V DOUBLE,
  TA  DOUBLE,

  GapC  DOUBLE,
  Gap  DOUBLE,
  GapL  DOUBLE,
  GapH  DOUBLE,

  OI DOUBLE,
  OIDiff DOUBLE,
  OIGap DOUBLE,

  UN STRING,

  N50 INT,
      N50T INT,
        BNF INT,
          BNFT INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'candles-live2',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);
