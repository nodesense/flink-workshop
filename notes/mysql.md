

open mysql console in portainer

mysql -uroot -proot

show databases;

use test;

show tables;

CREATE TABLE test.person (
PersonID int,
Name varchar(255)
);

INSERT INTO test.person (PersonID, Name)
VALUES (1, 'Tom B. Erichsen');

CREATE TABLE test.person2 (
PersonID int,
Name varchar(255)
);


SELECT * FROM test.person;
SELECT * FROM test.person2;

We will copy data from person 1 to person2...
copy movie sql/anything from flink table , can be copied into mysql or
read from mysql and process data