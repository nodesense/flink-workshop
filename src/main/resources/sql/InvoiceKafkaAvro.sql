CREATE TABLE Invoices (
 `id` STRING,
  qty INT,
  amount DOUBLE,
  customerId STRING,
  state STRING,
  country STRING,
  invoiceDate BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'invoices',
  'properties.bootstrap.servers' = 'localhost:9092',
  -- UTF-8 string as Kafka keys, using the 'the_kafka_key' table column
  'key.format' = 'raw',
  'key.fields' = 'country',
  'properties.group.id'='test',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://localhost:8081',
  'value.fields-include' = 'EXCEPT_KEY',
   'scan.startup.mode' = 'latest-offset',
   'properties.auto.offset.reset' = 'latest'
)