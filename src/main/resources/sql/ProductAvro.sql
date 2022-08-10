CREATE TABLE products (
  `id` INT,
  name STRING,
  price INT
) WITH (
 'connector' = 'kafka',
 'topic' = 'products',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'product-group-1',
   'scan.startup.mode' = 'earliest-offset',


  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://localhost:8081'

)