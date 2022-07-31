package workshop.analytics;


import workshop.models.DataValue;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.functions.MaxWindowFunction;
import workshop.util.SqlText;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ReadJsonWindowCustomProcessor {
    public static void main(String[] args) throws  Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String jsonSql = SqlText.getSQL("/sql/DataValuesJson.sql");
        System.out.println(jsonSql);

        tableEnv.executeSql(jsonSql);

        // union the two tables
        final Table result =  tableEnv.sqlQuery("SELECT * FROM DataValues");

        // convert the Table back to an insert-only DataStream of type `Order`
        DataStream<DataValue> dataValueStream = tableEnv.toDataStream(result, DataValue.class);

        dataValueStream.print();

        // Long??
        WatermarkStrategy< DataValue> watermarkStrategy =  WatermarkStrategy
                .<DataValue>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner((event, timestamp) -> event.ts.getTime());

        DataStream<DataValue>  dataValueStreamWithWaterMark = dataValueStream
                                        .assignTimestampsAndWatermarks(watermarkStrategy);



        final StreamingFileSink<DataValue> fileSink = StreamingFileSink
                .forRowFormat(new Path("/home/krish/IdeaProjects/FlinkDemo/outputs"), new SimpleStringEncoder<DataValue>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();


        SingleOutputStreamOperator<DataValue> dvOutputStream = dataValueStreamWithWaterMark
                .keyBy( (event) -> event.tag )
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new MaxWindowFunction())
                ;

        dvOutputStream.addSink(fileSink);

        PrintSinkFunction<DataValue> printFunction = new PrintSinkFunction<>();

        dvOutputStream.addSink(printFunction);

        // after the table program is converted to a DataStream program,
        // we must use `env.execute()` to submit the job
        env.execute();

    }
}
