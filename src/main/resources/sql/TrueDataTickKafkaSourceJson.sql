CREATE TABLE TrueDataTicks (
    Symbol STRING,
    Symbol_ID BIGINT,

    `Timestamp` TIMESTAMP_LTZ,
     LTP DOUBLE,
     LTQ BIGINT,
     ATP DOUBLE,
     Volume BIGINT,
     `Open` DOUBLE,
     High DOUBLE,
     Low DOUBLE,
     Prev_Close DOUBLE,


      OI BIGINT,
      Prev_Open_Int_Close BIGINT,

      Day_Turnover DOUBLE,

      Special STRING,
      Tick_Sequence_No BIGINT,

        Bid DOUBLE,

        Bid_Qty BIGINT,

        Ask DOUBLE,

         Ask_Qty BIGINT
) WITH (
  'connector' = 'kafka',
   'topic' = 'nse-live-ticks3',
   'properties.bootstrap.servers' = 'broker:9092',
    'properties.group.id' = 'my-candle-consumer-group5',
     'scan.startup.mode' = 'latest-offset',
   'value.format' = 'json',
   'value.json.timestamp-format.standard' = 'ISO-8601'
);
