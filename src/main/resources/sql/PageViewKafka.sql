CREATE TABLE IF NOT EXISTS PageViewKafka (
   `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    viewtime BIGINT,
    userid STRING,
    pageid STRING
     ) WITH (
     'connector' = 'kafka',
        'topic' = 'pageviews-json',
        'properties.bootstrap.servers' = 'broker:9092',
         'properties.group.id' = 'pageviews-json-flink-consumer',
          'scan.startup.mode' = 'latest-offset',
        'value.format' = 'json',
        'value.json.timestamp-format.standard' = 'ISO-8601'
     );
