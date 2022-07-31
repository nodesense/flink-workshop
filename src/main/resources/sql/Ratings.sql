CREATE TABLE Ratings (
  `userId` INT,
  `movieId` INT,
  `rating` DOUBLE,
  `timestamp` BIGINT
) WITH (
 'connector' = 'filesystem',
 'format' = 'csv',
 'path' = 'data/ml-latest-small/ratings.csv',
 'csv.ignore-parse-errors' = 'true',
  'csv.allow-comments' = 'true'
);