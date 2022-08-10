CREATE TABLE CandleParquetPartition (
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
          BNFT INT,

  dt STRING
) PARTITIONED BY (asset, dt) WITH (
  'connector' = 'filesystem',
  'path' = 'output/parquet-partitions',
  'format' = 'parquet'
);