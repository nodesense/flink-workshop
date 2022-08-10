stock market
2000 plus symbols
daily .. timeseries

/parquet   
file1.parquet
file2.parquet
....

if no partitions, when we run the queries, all files in parquest directory shall be scanned
only specific columns read, but it reads all files..
SELCT ...[some columns] from .... WHERE ...[some column]

PARTITION [concept idea from hive], we can use without hive installed

When we run the query, if asset column present in WHERE, GROUP BY etc , then
flink runtime only pick respective folders, not reading from other symbols/assets.

SELECT ... from .. WHERE  asset='INFY' -- it only read from asset=INFY directory, not from other directory..


/parquet-partitions-symbols
/asset=INFY
file1.parquet
file2.parquet
..

    /asset=SBIN
            file1.parquet
            file2.parquet
            ..
    ...

PARTITION with datetime along with sensorid/asset/tag etc

SELECT .... from .. WHERE asset=INFY and dt=2022-08-14 -- read the data from INFY, on date Aug 14th 2022

/parquet-partitions-symbols-date
/asset=INFY
/dt=2022-08-08
file1.parquet
file2.parquet
..
...
/dt=2022-08-14
file1.parquet
file2.parquet
..

         /dt=2022-08-16
            file1.parquet
            file2.parquet
            ..

    /asset=SBIN
            file1.parquet
            file2.parquet
            ..
    ...
