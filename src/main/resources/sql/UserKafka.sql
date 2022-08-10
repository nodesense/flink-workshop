CREATE TABLE IF NOT EXISTS UserKafka (
    registertime BIGINT,
    userid STRING,
    regionid STRING,
    gender STRING
     ) WITH (
     'connector' = 'kafka',
        'topic' = 'users-json',
        'properties.bootstrap.servers' = 'broker:9092',
         'properties.group.id' = 'user-json-flink-consumer',
          'scan.startup.mode' = 'latest-offset',
        'value.format' = 'json',
        'value.json.timestamp-format.standard' = 'ISO-8601'
     );
