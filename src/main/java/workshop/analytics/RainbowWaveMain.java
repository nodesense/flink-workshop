package workshop.analytics;


import workshop.models.DataValueMap;
import workshop.models.Tick;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.functions.RainbowTickWaveWindow;
import workshop.util.SqlText;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class RainbowWaveMain {
    public static void main(String[] args) throws  Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String jsonSql = SqlText.getSQL("/sql/TickSourceJson.sql");
        System.out.println(jsonSql);

        String jsonSyncSql = SqlText.getSQL("/sql/RainbowSinkJson.sql");
        System.out.println(jsonSyncSql);

        tableEnv.executeSql(jsonSql);

        tableEnv.executeSql(jsonSyncSql);

        // union the two tables
        final Table result =  tableEnv.sqlQuery("SELECT * FROM Ticks");

        // convert the Table back to an insert-only DataStream of type `Order`
        DataStream<Tick> dataValueStream = tableEnv.toDataStream(result, Tick.class);

        dataValueStream.print();

        // Long??
        WatermarkStrategy<Tick> watermarkStrategy =  WatermarkStrategy
                .<Tick>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner((event, timestamp) -> event.ts.getTime());

        DataStream<Tick>  dataValueStreamWithWaterMark = dataValueStream
                                        .assignTimestampsAndWatermarks(watermarkStrategy);



        final StreamingFileSink<DataValueMap> fileSink = StreamingFileSink
                .forRowFormat(new Path("/home/krish/IdeaProjects/FlinkDemo/outputs"), new SimpleStringEncoder<DataValueMap>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();


        SingleOutputStreamOperator<DataValueMap> dvOutputStream = dataValueStreamWithWaterMark
                .keyBy( (event) -> event.asset )
               // .process(new TAWindow("SMA", 5))
                .process(new RainbowTickWaveWindow("DEMA", 2))
                ;

        //dvOutputStream.addSink(fileSink);

        PrintSinkFunction<DataValueMap> printFunction = new PrintSinkFunction<>();

       // dvOutputStream.addSink(printFunction);

        Table resultTable = tableEnv.fromDataStream(dvOutputStream);

        // register the Table object as a view and query it
        tableEnv.createTemporaryView("rainbow_waves", resultTable);

        resultTable.printSchema();

        // tableEnv.executeSql("SELECT asset, ts,  wave1 from rainbow_waves").print();

        tableEnv.executeSql("INSERT INTO Rainbows select asset, ts, wave1, wave2, wave3,wave4,wave5,wave6,wave7,wave8,wave9,wave10,wave11,wave12,wave3,wave4,wave15,wave16,wave17,wave18,wave19,wave20,wave21,wave22,wave23,wave24,adiff,sdiff,signal, williamr, cci, rsi, kcm, kcu, kcl, roc, ltp, arup, ardown, aroc, lreg from rainbow_waves").wait();
        resultTable.executeInsert("Rainbows");

        // resultTable.execute().print();
        // after the table program is converted to a DataStream program,
        // we must use `env.execute()` to submit the job
        env.execute();
    }
}
