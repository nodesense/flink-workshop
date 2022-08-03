CREATE TABLE IF NOT EXISTS  MoviesHiveHadoop (
  movieId INT,
  title STRING,
  genres STRING
) WITH (
 'connector' = 'filesystem',
 'format' = 'csv',
 'path' = 'hdfs://namenode:9000/ml-latest-small/movies.csv',
 'csv.ignore-parse-errors' = 'true',
  'csv.allow-comments' = 'true'
);