CREATE TABLE Movies (
  movieId INT,
  title STRING,
  genres STRING
) WITH (
 'connector' = 'filesystem',
 'format' = 'csv',
 'path' = 'data/ml-latest-small/movies.csv',
 'csv.ignore-parse-errors' = 'true',
  'csv.allow-comments' = 'true'
);