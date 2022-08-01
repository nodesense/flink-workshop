
# setup 
 
```
sudo apt install openjdk-11-jdk

```

 

1. CSV example in Java
2. JSON Example in Java
3. JDBC Example in Java
4. Hive Example in Java
5. Kafka Example in Java
6. SQLs
7. HDFS EXamples 

,
`fields` MAP <STRING, DOUBLE>


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
