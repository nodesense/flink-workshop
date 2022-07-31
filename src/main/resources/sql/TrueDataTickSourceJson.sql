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
 'connector' = 'filesystem',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true',
 'path' = 'file:///media/krish/IntelNVMe/truedata/flink-ticks-json/2022-07-07.json',
 'json.timestamp-format.standard' = 'ISO-8601'
);
