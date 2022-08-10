
# setup 
 
```
sudo apt install openjdk-11-jdk

```

```
sudo nano /etc/hosts
```

paste below content..

Ctrl + O then Press Enter to save

Ctrl + X to exit


```
127.0.0.1 broker
127.0.0.1 schema-registry
127.0.0.1 namenode
127.0.0.1 hive-metastore
```
 

```
wget -P mount/jars https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/1.15.0/flink-connector-jdbc-1.15.0.jar
wget -P mount/jars https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.29/mysql-connector-java-8.0.29.jar
wget  -P mount/jars https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop2-uber/2.7.5-1.8.3/flink-shaded-hadoop2-uber-2.7.5-1.8.3.jar

wget  -P mount/jars  https://repo1.maven.org/maven2/org/apache/flink/flink-table-api-java-bridge/1.15.1/flink-table-api-java-bridge-1.15.1.jar

wget  -P mount/jars https://repo1.maven.org/maven2/org/apache/flink/flink-connector-hive_2.12/1.15.1/flink-connector-hive_2.12-1.15.1.jar
wget  -P mount/jars https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.6/hive-exec-2.3.6.jar

wget  -P mount/jars https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.15.1/flink-sql-connector-kafka-1.15.1.jar

wget  -P mount/jars https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.15.1/flink-parquet-1.15.1.jar
wget  -P mount/jars https://repo1.maven.org/maven2/org/apache/flink/flink-orc/1.15.1/flink-orc-1.15.1.jar
```



$FLINK_HOME/bin/flink run \
--detached \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar




$FLINK_HOME/bin/flink run -c workshop.stream.S043_ContinuousFileProcessing \
--detached \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar



$FLINK_HOME/bin/flink run -c workshop.stream.S043_ContinuousFileProcessing \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar


$FLINK_HOME/bin/flink run -c examples.FlinkCSVExample \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar


http://0.0.0.0:49232

# Submit to job manager on differnt host

$FLINK_HOME/bin/flink run  -m 0.0.0.0:49232 \
--detached \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar


./bin/flink run \
--detached \
./examples/streaming/StateMachineExample.jar

./bin/flink list

./bin/flink savepoint \
$JOB_ID \
/tmp/flink-savepoints

./bin/flink savepoint \
--dispose \
/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \
$JOB_ID

./bin/flink stop \
--savepointPath /tmp-flink-savepoints \
$JOB_ID

./bin/flink run \
--detached \
--fromSavepoint /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \
./examples/streaming/StateMachineExample.jar


For code sharing..

https://bit.ly/3ON1RVs


$FLINK_HOME/bin/flink run  -m 0.0.0.0:49275 \
--detached \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar





$FLINK_HOME/bin/flink run  -m 0.0.0.0:8181 -c workshop.analytics.TrueDataCandleMain \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar


$FLINK_HOME/bin/flink run  -m 0.0.0.0:8181 -c workshop.analytics.TrueDataCandleMain \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar



$FLINK_HOME/bin/flink run  -m 0.0.0.0:8181 -c workshop.table.S031_HelloWorld \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar







$FLINK_HOME/bin/flink run  -m 0.0.0.0:8181 -c workshop.stream.S041_ReadNumbersFromFile \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar


$FLINK_HOME/bin/flink run  -m 0.0.0.0:8181 -c workshop.stream.S046_StreamSQLExample \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar



$FLINK_HOME/bin/flink run  -m 0.0.0.0:8181 -c workshop.stream.S047_StreamWindowSQLExample \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar


$FLINK_HOME/bin/flink run  -m 0.0.0.0:8181 -c workshop.table.S034_FlinkCSVExample \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar



$FLINK_HOME/bin/flink run  -m 0.0.0.0:8181 -c workshop.table.S035_MovieLensAnalytics \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar


$FLINK_HOME/bin/flink run  -m 0.0.0.0:8181 -c workshop.hive.HiveCatalogTest \
./target/FlinkAnalytics-1.0-SNAPSHOT.jar