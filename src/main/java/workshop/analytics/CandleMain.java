package workshop.analytics;


import workshop.models.Tick;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.util.SqlText;

import java.time.Duration;

public class CandleMain {
    public static void main(String[] args) throws  Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String jsonSql = SqlText.getSQL("/sql/TickSourceJson.sql");
        System.out.println(jsonSql);

        String jsonSyncSql = SqlText.getSQL("/sql/CandleSinkJson.sql");
        System.out.println(jsonSyncSql);

        tableEnv.executeSql(jsonSql);

        tableEnv.executeSql(jsonSyncSql);

        // union the two tables
        final Table result =  tableEnv.sqlQuery("SELECT * FROM Ticks where `asset` = 'NIFTY 50' ");

        // convert the Table back to an insert-only DataStream of type `Order`
        DataStream<Tick> tickDataStream = tableEnv.toDataStream(result, Tick.class)
                        .filter(tick -> tick.asset == "NIFTY 50");

        tickDataStream.print();

        // Long??
        WatermarkStrategy<Tick> watermarkStrategy =  WatermarkStrategy
                .<Tick>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, timestamp) -> event.ts.getTime());

        DataStream<Tick>  dataValueStreamWithWaterMark = tickDataStream
                                        .assignTimestampsAndWatermarks(watermarkStrategy);


//
//        final StreamingFileSink<TickTA> fileSinkNormal = StreamingFileSink
//                .forRowFormat(new Path("/home/krish/IdeaProjects/FlinkDemo/outputs"), new SimpleStringEncoder<TickTA>("UTF-8"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//                                .withMaxPartSize(1024 * 1024 * 1024)
//                                .build())
//                .build();
//
//
//
//        final StreamingFileSink<TickTA> fileSink = StreamingFileSink
//                .forRowFormat(new Path("/home/krish/IdeaProjects/FlinkDemo/outputs"), new SimpleJsonEncoder<TickTA>())
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
//                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//                                .withMaxPartSize(1024 * 1024 * 1024)
//                                .build())
//                .build();
//
//
//        SingleOutputStreamOperator<Candle> candleStream = dataValueStreamWithWaterMark
//                .keyBy( (tick) -> tick.asset )
//                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//
//               // .process(new TAWindow("SMA", 5))
//                .process(new CandleWindowFunction("OohMyName.Fun"))
//                ;
//
//
//        SingleOutputStreamOperator<TickTA> tickTAStream =  candleStream
//                .keyBy( (tickTA) -> tickTA.asset )
//                .process(new CandleAnalysisWindow("Welcome", 5));
//
//        tickTAStream.addSink(fileSink);
//
//        PrintSinkFunction<TickTA> printFunction = new PrintSinkFunction<>();

     //  tickTAStream.addSink(printFunction);

//        Table resultTable = tableEnv.fromDataStream(candleStream);
//
//        // register the Table object as a view and query it
//        tableEnv.createTemporaryView("candle_temp", resultTable);
//
//        resultTable.printSchema();


        // tableEnv.executeSql("SELECT asset, st,  et, O, C, H, L, V, TV from rainbow_waves").print();
//
//        tableEnv.executeSql("INSERT INTO Candles select  asset, st,  et, O, C, H, L, V, TA     from candle_temp").wait();
//        resultTable.executeInsert("Candles");
//
//
//
//        SingleOutputStreamOperator<DataValueMap> dvOutputStream = candleStream
//                                                            .keyBy( (candle) -> candle.asset )
//                                                            .process(new RainbowCandleWaveWindow("OohMyName.Fun", 5))
//                                                            ;
//
//        Table resultTable2 = tableEnv.fromDataStream(dvOutputStream);
//
//        dvOutputStream.print();
//
//        // register the Table object as a view and query it
//        tableEnv.createTemporaryView("rainbow_waves", resultTable2);
//
//        resultTable.printSchema();
//
//        // tableEnv.executeSql("SELECT asset, ts,  wave1 from rainbow_waves").print();
//
//        tableEnv.executeSql("INSERT INTO Rainbows select asset, ts, wave1, wave2, wave3,wave4,wave5,wave6,wave7,wave8,wave9,wave10,wave11,wave12,wave3,wave4,wave15,wave16,wave17,wave18,wave19,wave20,wave21,wave22,wave23,wave24,adiff,sdiff,signal, williamr, cci, rsi from rainbow_waves").wait();
//        resultTable.executeInsert("Rainbows");


        // resultTable.execute().print();
        // after the table program is converted to a DataStream program,
        // we must use `env.execute()` to submit the job
        env.execute();
    }
}
