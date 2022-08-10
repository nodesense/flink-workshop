CREATE TABLE IF NOT EXISTS PageViewKafka (
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
