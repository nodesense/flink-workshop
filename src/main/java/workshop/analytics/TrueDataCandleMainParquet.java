package workshop.analytics;


import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.functions.TickWindowDemo;
import workshop.models.Candle;
import workshop.models.TrueDataTick;
import workshop.util.SqlText;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

// FQN - Fully Quafied name to be given while subbmit flink job
// workshop.analytics.TrueDataCandleMain
public class TrueDataCandleMainParquet {
    public static void main(String[] args) throws  Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String TrueDataTickKafkaSourceJson = SqlText.getSQL("/sql/TrueDataTickKafkaSourceJson.sql");
        System.out.println(TrueDataTickKafkaSourceJson);
        tableEnv.executeSql(TrueDataTickKafkaSourceJson);
        // Stream tables are called dynamic tables
        //stream tables shall have change log, newly added reocrd/modified records are published
        final Table result =  tableEnv.sqlQuery("SELECT * FROM TrueDataTicks");
        // result.execute().print();
        // type cast Table Row into Java POJO

        DataStream<TrueDataTick> tickDataStream = tableEnv.toDataStream(result, TrueDataTick.class);
        // This creates a flow graph
        // Kafka as source and printfunction as sink
        // lazy evaluation, we need to execute it on job manager
        // tickDataStream.print();

        // watermark
        // processing time flink time when the data processed
        // event time - when the data proceduced
//        WatermarkStrategy<TrueDataTick> watermarkStrategy =  WatermarkStrategy
//        .<TrueDataTick>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//        .withTimestampAssigner((event, timestamp) -> event.Timestamp.getTime());

        WatermarkStrategy<TrueDataTick> watermarkStrategy = new WatermarkStrategy<TrueDataTick>() {
            @Override
            public WatermarkGenerator<TrueDataTick> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new BoundedOutOfOrdernessWatermarks<TrueDataTick>(Duration.ofSeconds(20)) {
                    @Override
                    public void onEvent(TrueDataTick event, long eventTimestamp, WatermarkOutput output) {
                        // System.out.println("Inside water marke geneeator " +  event);
                        // custom code that force the window to process the data next
                        super.onEvent(event, eventTimestamp, output);
                        super.onPeriodicEmit(output);
                    }
                };
            }
        }
                .withTimestampAssigner((event, timestamp) -> event.Timestamp.getTime());

        // payload, special tag, attribute, that may say that process the message
        // start transaction itself an event
        //   followed by messages based on tracsaction ..
        // end transaction , another event

        DataStream<TrueDataTick>  dataValueStreamWithWaterMark = tickDataStream
                .assignTimestampsAndWatermarks(watermarkStrategy);// crete

        dataValueStreamWithWaterMark.print();




        SingleOutputStreamOperator<Candle> candleStream = dataValueStreamWithWaterMark
                .keyBy( (tick) -> tick.Symbol )
                //  .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(1))) // results producerd  1 sec offset past time interval like 11 instead of 10
                // .window(TumblingEventTimeWindows.of(Time.seconds(2))) // rounded to wall clock 5, 10, 15, ...
                // .process(new TAWindow("SMA", 5))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new TickWindowDemo())
                ;


        candleStream.print(); // no collect is used, won't print anything

        // finally publish candles to kafka

        Table candleTempTable = tableEnv.fromDataStream(candleStream);

        // register the Table object as a view and query it
        tableEnv.createTemporaryView("TempCandles", candleTempTable);

        String CandleKafka = SqlText.getSQL("/sql/CandleKafka.sql");
        System.out.println(CandleKafka);

        tableEnv.executeSql(CandleKafka);

        // INSERT INTO CandleKafka is a data flow graph
        //  FROM is source table TempCandles
        // SELECT..complex queries are transformation, aggregation etc...
        //   INTO table CandleKafka is sink table

        TableResult result2 = tableEnv.executeSql("INSERT INTO CandleKafka SELECT `asset`, st, et, O, C, H, L, A ,DT, V, TA,  GapC, Gap , GapL , GapH, OI, OIDiff, OIGap, UN, N50, N50T, BNF, BNFT FROM TempCandles");



        String CandleParquet = SqlText.getSQL("/sql/CandleParquet.sql");
        System.out.println(CandleParquet);

        tableEnv.executeSql(CandleParquet);

        TableResult result3 = tableEnv.executeSql("INSERT INTO CandleParquet SELECT `asset`, st, et, O, C, H, L, A ,DT, V, TA,  GapC, Gap , GapL , GapH, OI, OIDiff, OIGap, UN, N50, N50T, BNF, BNFT FROM TempCandles");


        String CandleParquetPartition = SqlText.getSQL("/sql/CandleParquetPartitions.sql");
        System.out.println(CandleParquetPartition);

        tableEnv.executeSql(CandleParquetPartition);

        TableResult result4 = tableEnv.executeSql("INSERT INTO CandleParquetPartition SELECT `asset`, st, et, O, C, H, L, A ,DT, V, TA,  GapC, Gap , GapL , GapH, OI, OIDiff, OIGap, UN, N50, N50T, BNF, BNFT, DATE_FORMAT(et, 'yyyy-MM-dd') as dt  FROM TempCandles");



        // get the graph and submit to job manager
        env.execute();

        //
        try {
            JobClient jobClient = result2.getJobClient().get();
            System.out.println(" JOB ID " + result2.getJobClient().get().getJobID());

            Thread.sleep(30 * 1000);
            System.out.println("Now cancelling job " + System.currentTimeMillis());

            // will not wait for result or cancellation
            //  jobClient.cancel();

            // Sync wait for cancel to happen, blocking call
            jobClient.cancel().get(); // blocking call, wait until job cancelled
            System.out.println("Job cancelled successfully " + System.currentTimeMillis());

            // async, not blocking confirmation using java future
            //  CompletableFuture<Void> cancelFuture = jobClient.cancel();
//             cancelFuture.completeAsync(new Supplier<Void>() {
//                 @Override
//                 public Void get() {
//                     System.out.println("Job cancelled successfully");
//                     return null;
//                 }
//             });


            //  System.out.println("Job cancelled successfully " + System.currentTimeMillis());

        }catch (Exception ex) {
            System.out.println(ex);
            ex.printStackTrace();
        }


    }

}
