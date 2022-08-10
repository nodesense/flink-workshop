package workshop.analytics;

/*
# JSON

ksql-datagen quickstart=users format=json topic=users-json maxInterval=60000 iterations=50000
ksql-datagen quickstart=pageviews format=json topic=pageviews-json maxInterval=60000 iterations=50000
ksql-datagen quickstart=orders  format=json topic=orders-json maxInterval=60000 iterations=50

# Avro

ksql-datagen quickstart=users format=avro topic=users-avro maxInterval=60000 iterations=50
ksql-datagen quickstart=pageviews format=avro topic=pageviews-avro maxInterval=60000 iterations=50
ksql-datagen quickstart=orders  format=avro topic=orders-avro maxInterval=60000 iterations=50

        ----

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic users-avro --from-beginning --property schema.registry.url="http://localhost:8081"
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic pageviews-avro --from-beginning --property schema.registry.url="http://localhost:8081"
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic orders-avro --from-beginning --property schema.registry.url="http://localhost:8081"

        ---


kafka-console-consumer --bootstrap-server localhost:9092 --topic users --from-beginning --property schema.registry.url="http://localhost:8081"
kafka-console-consumer --bootstrap-server localhost:9092 --topic pageviews --from-beginning --property schema.registry.url="http://localhost:8081"
kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-avro --from-beginning --property schema.registry.url="http://localhost:8081"


*/

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.functions.TickWindowDemo;
import workshop.models.*;
import workshop.util.SqlText;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class UserPageViewsJoin {
    public static void main(String[] args) throws Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String UserKafka = SqlText.getSQL("/sql/UserKafka.sql");
        System.out.println(UserKafka);
        tableEnv.executeSql(UserKafka);
        // Stream tables are called dynamic tables
        //stream tables shall have change log, newly added reocrd/modified records are published
        final Table usersKafkaTable =  tableEnv.sqlQuery("SELECT * FROM UserKafka");

        WatermarkStrategy<User> userWatermarkStrategy =  WatermarkStrategy
                .<User>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                // .forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.registertime);

        DataStream<User> userDataStream = tableEnv.toDataStream(usersKafkaTable, User.class)
                .assignTimestampsAndWatermarks(userWatermarkStrategy);



        String PageViewKafka = SqlText.getSQL("/sql/PageViewKafka.sql");
        System.out.println(PageViewKafka);
        tableEnv.executeSql(PageViewKafka);
        // Stream tables are called dynamic tables
        //stream tables shall have change log, newly added reocrd/modified records are published
        final Table pageViewKafkaTable =  tableEnv.sqlQuery("SELECT * FROM PageViewKafka");


        WatermarkStrategy<PageView> pageViewWatermarkStrategy =  WatermarkStrategy
                .<PageView>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                // .forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.viewtime);

        DataStream<PageView> pageviewDataStream = tableEnv.toDataStream(pageViewKafkaTable, PageView.class)
                        .assignTimestampsAndWatermarks(pageViewWatermarkStrategy);




        userDataStream.print();
        pageviewDataStream.print();

        DataStream<PageViewUser> userPageViewStream = pageviewDataStream // left
                .join(userDataStream) // right
                .where (pageView -> pageView.userid) // left
                .equalTo(user -> user.userid) // right
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                        .apply(new JoinFunction<PageView, User, PageViewUser>() {
                            @Override
                            public PageViewUser join(PageView pageView, User user) throws Exception {
                                PageViewUser pageViewUser = new PageViewUser();
                                pageViewUser.gender = user.gender;
                                pageViewUser.userid = user.userid;
                                pageViewUser.regionid = user.regionid;
                                pageViewUser.pageid = pageView.pageid;
                                pageViewUser.viewtime = pageView.viewtime;
                                return pageViewUser;
                            }
                        });

        userPageViewStream.print();

        env.execute();


    }
}
