package workshop.analytics;


import org.apache.flink.streaming.api.CheckpointingMode;
import workshop.models.TrueDataTick;
import workshop.models.Candle;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.functions.CandleWindowFunction;
import workshop.util.SqlText;

import java.time.Duration;

public class TrueDataCandleMain {
    public static void main(String[] args) throws  Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        // start a checkpoint every 1000 ms
//    env.enableCheckpointing(10000);
//
//
//        // set mode to exactly-once (this is the default)
  //   env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//
//        // sets the checkpoint storage where checkpoint snapshots will be written
//        // shall use hdfs host name
    //env.getCheckpointConfig().setCheckpointStorage("file:///nfs/checkpoint/candle");
      //  env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:9000/checkpoint/candle4");


        // EnvironmentSettings env = EnvironmentSettings.inStreamingMode();
        // StreamExecutionEnvironment env
    //     env.getConfig().setAutoWatermarkInterval(Duration.ofMillis(100).toMillis());
      //  env.getConfig().setLocalTimeZone(ZoneId.of("UTC"));

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String TrueDataTickKafkaSourceJson = SqlText.getSQL("/sql/TrueDataTickKafkaSourceJson.sql");
        System.out.println(TrueDataTickKafkaSourceJson);
        tableEnv.executeSql(TrueDataTickKafkaSourceJson);

//
//        // union the two tables
        final Table result =  tableEnv.sqlQuery("SELECT * FROM TrueDataTicks");
     //   result.execute().print();
//
//        // convert the Table back to an insert-only DataStream of type `Order`
          DataStream<TrueDataTick> tickDataStream = tableEnv.toDataStream(result, TrueDataTick.class);
            tickDataStream.print();


//
//        Properties kafkaProperties = new Properties();
//
//        kafkaProperties.put("ClassType", TrueDataTick.class);
//        kafkaProperties.put("kafka.topic", "nse-live-ticks3");
//        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
//        JsonKafkaSource<TrueDataTick> kafkaSource = new JsonKafkaSource<>(env, tableEnv);
//        DataStream<TrueDataTick> tickDataStream = kafkaSource.load(kafkaProperties);

      //    tickDataStream.print();

//            properties.setProperty("bootstrap.servers", "localhost:9092");
//
//           DataStream<Tick> tickDataStream = kafkaSource.load(properties);


        //tickDataStream.print();

//        Properties kafkaProperties = new Properties();
//
//        kafkaProperties.put("ClassType", TrueDataTick.class);
//        kafkaProperties.put("kafka.topic", "nse-live-ticks2");
//
//        //JsonFileSource<TrueDataTick> tickJsonSource = new JsonFileSource<TrueDataTick>(env, tableEnv);
//
//
//        JsonKafkaSource<TrueDataTick> kafkaSource = new JsonKafkaSource<>(env, tableEnv);
//        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
//
//           DataStream<TrueDataTick> tickDataStream = kafkaSource.load(kafkaProperties);
//
         tickDataStream.print();


//            // Long??

     //   DataStream<TrueDataTick>   tickDataStream = tickJsonSource.load (kafkaProperties);

//        String jsonSql = SqlText.getSQL("sql/TrueDataTickSourceJson.sql");
//        System.out.println(jsonSql);
//        tableEnv.executeSql(jsonSql);
        //final Table result =  tableEnv.sqlQuery("SELECT * FROM TrueDataTicks where `Symbol` = 'NIFTY 50' ");
//        final Table result =  tableEnv.sqlQuery("SELECT * FROM TrueDataTicks");
//        // convert the Table back to an insert-only DataStream of type `Order`
//        DataStream<TrueDataTick> tickDataStream = tableEnv.toDataStream(result, TrueDataTick.class);
//        //    .filter(tick -> tick.Symbol == "NIFTY 50");



        String jsonSyncSql = SqlText.getSQL("/sql/CandleSinkJson.sql");
        System.out.println(jsonSyncSql);
        tableEnv.executeSql(jsonSyncSql);
        // union the two tables



     //   tickDataStream.print();

        // Long??
//        WatermarkStrategy<TrueDataTick> watermarkStrategy =  WatermarkStrategy
//                .<TrueDataTick>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//               // .forMonotonousTimestamps()
//    .withTimestampAssigner((event, timestamp) -> event.Timestamp.getTime());

        WatermarkStrategy<TrueDataTick> watermarkStrategy = new WatermarkStrategy<TrueDataTick>() {
            @Override
            public WatermarkGenerator<TrueDataTick> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new BoundedOutOfOrdernessWatermarks<TrueDataTick>(Duration.ofSeconds(2)) {
                    @Override
                    public void onEvent(TrueDataTick event, long eventTimestamp, WatermarkOutput output) {

                   //     System.out.println("Inside water marke geneeator " +  event);
                        super.onEvent(event, eventTimestamp, output);
                        super.onPeriodicEmit(output);
                    }
                };
            }
        }
        .withTimestampAssigner((event, timestamp) -> event.Timestamp.getTime());


        DataStream<TrueDataTick>  dataValueStreamWithWaterMark = tickDataStream
                                       .assignTimestampsAndWatermarks(watermarkStrategy);// crete


        SingleOutputStreamOperator<Candle> candleStream = dataValueStreamWithWaterMark
                .keyBy( (tick) -> tick.Symbol )
              //  .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(1))) // results producerd  1 sec offset past time interval like 11 instead of 10
               // .window(TumblingEventTimeWindows.of(Time.seconds(2))) // rounded to wall clock 5, 10, 15, ...
               // .process(new TAWindow("SMA", 5))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new CandleWindowFunction("OohMyName.Fun"))
                ;



       candleStream.print();




        //candleStream.addSink(fileSink);

        PrintSinkFunction<Candle> printFunction = new PrintSinkFunction<>();
        candleStream.addSink(printFunction);

       Table candleTempTable = tableEnv.fromDataStream(candleStream);

        // register the Table object as a view and query it
        tableEnv.createTemporaryView("TempCandles", candleTempTable);

        String CandleKafka = SqlText.getSQL("/sql/CandleKafka.sql");
        System.out.println(CandleKafka);

        tableEnv.executeSql(CandleKafka);

        tableEnv.executeSql("INSERT INTO CandleKafka SELECT `asset`, st, et, O, C, H, L, A ,DT, V, TA,  GapC, Gap , GapL , GapH, OI, OIDiff, OIGap, UN, N50, N50T, BNF, BNFT FROM TempCandles");
//

        String TotalAggregateKafka = SqlText.getSQL("/sql/TotalAggregateKafka.sql");
        System.out.println(TotalAggregateKafka);

        tableEnv.executeSql(TotalAggregateKafka);

        final Table result1 =  tableEnv.sqlQuery("SELECT 'NIFTY BANK EQ' as `asset` ,  st, et, DT, sum(V) as V, sum(TA) as TA, avg(OI) as OI, sum(OIDiff) as OIDiff FROM TempCandles where DT='EQ' and  BNF=1 GROUP BY DT, st, et");
        // result1.execute().print();

        tableEnv.createTemporaryView("NIFTYBANKEQAggregate", result1);
        tableEnv.executeSql("INSERT INTO TradeAggregates SELECT `asset`, st, et, DT, V, TA, OI, OIDiff FROM NIFTYBANKEQAggregate");

        // Sum all volumes, TA based on various interests and union the results, write back again..
        // Sum volumes for equities for N50
        final Table result2 =  tableEnv.sqlQuery("SELECT CONCAT('NIFTY 50 EQ','') as `asset` ,  st, et, DT, sum(V) as V, sum(TA) as TA, avg(OI) as OI, sum(OIDiff) as OIDiff FROM TempCandles where DT='EQ' and  N50=1 GROUP BY DT, st, et");
       // result2.execute().print();

        tableEnv.createTemporaryView("NIFTY50EQAggregate", result2);
        tableEnv.executeSql("INSERT INTO TradeAggregates SELECT `asset`, st, et, DT, V, TA, OI, OIDiff FROM NIFTY50EQAggregate");

        final Table result3 =  tableEnv.sqlQuery("SELECT 'NIFTY 50 CE' as `asset` ,  st, et, DT, sum(V) as V, sum(TA) as TA, avg(OI) as OI, sum(OIDiff) as OIDiff  FROM TempCandles where DT='CE' and  UN='NIFTY 50' GROUP BY DT, st, et");
       // result3.execute().print();


        tableEnv.createTemporaryView("NIFTY50CEAggregate", result3);
        tableEnv.executeSql("INSERT INTO TradeAggregates SELECT `asset`, st, et, DT, V, TA, OI, OIDiff FROM NIFTY50CEAggregate");


        final Table result4 =  tableEnv.sqlQuery("SELECT 'NIFTY 50 PE' as `asset` ,  st, et, DT, sum(V) as V, sum(TA) as TA, avg(OI) as OI, sum(OIDiff) as OIDiff  FROM TempCandles where DT='PE' and  UN='NIFTY 50' GROUP BY DT, st, et");
      //  result4.execute().print();

        tableEnv.createTemporaryView("NIFTY50PEAggregate", result4);
        tableEnv.executeSql("INSERT INTO TradeAggregates SELECT `asset`, st, et, DT, V, TA, OI, OIDiff FROM NIFTY50PEAggregate");

        final Table result5 =  tableEnv.sqlQuery("SELECT 'NIFTY BANK PE' as `asset` ,  st, et, DT, sum(V) as V, sum(TA) as TA, avg(OI) as OI, sum(OIDiff) as OIDiff  FROM TempCandles where DT='PE' and  UN='NIFTY BANK' GROUP BY DT, st, et");
        // result5.execute().print();

        tableEnv.createTemporaryView("NIFTYBANKPEAggregate", result5);
        tableEnv.executeSql("INSERT INTO TradeAggregates SELECT `asset`, st, et, DT, V, TA, OI, OIDiff FROM NIFTYBANKPEAggregate");

        final Table result6 =  tableEnv.sqlQuery("SELECT 'NIFTY BANK CE' as `asset` ,  st, et, DT, sum(V) as V, sum(TA) as TA, avg(OI) as OI, sum(OIDiff) as OIDiff  FROM TempCandles where DT='CE' and  UN='NIFTY BANK' GROUP BY DT, st, et");
       // result6.execute().print();

        tableEnv.createTemporaryView("NIFTYBANKCEAggregate", result6);
        tableEnv.executeSql("INSERT INTO TradeAggregates SELECT `asset`, st, et, DT, V, TA, OI, OIDiff FROM NIFTYBANKCEAggregate");

        final Table result7 =  tableEnv.sqlQuery("SELECT 'NIFTY BANK FU' as `asset` ,  st, et, DT, sum(V) as V, sum(TA) as TA, avg(OI) as OI, sum(OIDiff) as OIDiff  FROM TempCandles where DT='FU' and  UN='NIFTY BANK' GROUP BY DT, st, et");
       //  result7.execute().print();

        tableEnv.createTemporaryView("NIFTYBANKFUAggregate", result7);
        tableEnv.executeSql("INSERT INTO TradeAggregates SELECT `asset`, st, et, DT, V, TA, OI, OIDiff FROM NIFTYBANKFUAggregate");

        final Table result8 =  tableEnv.sqlQuery("SELECT 'NIFTY FU' as `asset` ,  st, et, DT, sum(V) as V, sum(TA) as TA, avg(OI) as OI, sum(OIDiff) as OIDiff  FROM TempCandles where DT='FU' and  UN='NIFTY 50' GROUP BY DT, st, et");
     //   result5.execute().print();

        tableEnv.createTemporaryView("NIFTYFUAggregate", result8);
        tableEnv.executeSql("INSERT INTO TradeAggregates SELECT `asset`, st, et, DT, V, TA, OI, OIDiff FROM NIFTYFUAggregate");

        final Table result9 =  tableEnv.sqlQuery("SELECT 'NIFTY 50T' as `asset` ,  st, et, DT, sum(V) as V, sum(TA) as TA, avg(OI) as OI, sum(OIDiff) as OIDiff FROM TempCandles where DT='EQ' and  N50T=1 GROUP BY DT, st, et");
        //result9.execute().print();


        tableEnv.createTemporaryView("NIFTY50T", result9);
        tableEnv.executeSql("INSERT INTO TradeAggregates SELECT `asset`, st, et, DT, V, TA, OI, OIDiff FROM NIFTY50T");


        final Table result10 =  tableEnv.sqlQuery("SELECT 'NIFTY BANKT' as `asset` ,  st, et, DT, sum(V) as V, sum(TA) as TA, avg(OI) as OI, sum(OIDiff) as OIDiff FROM TempCandles where DT='EQ' and  BNFT=1 GROUP BY DT, st, et");
        //result9.execute().print();


        tableEnv.createTemporaryView("NIFTYBANKT", result10);
        tableEnv.executeSql("INSERT INTO TradeAggregates SELECT `asset`, st, et, DT, V, TA, OI, OIDiff FROM NIFTYBANKT");





//        // kafka-console-consumer --bootstrap-server localhost:9092 --topic  trade-aggregates
//        String KafkaTotalAggregateInsert = SqlText.getSQL("/sql/KafkaTotalAggregateInsert.sql");
//        System.out.println(KafkaTotalAggregateInsert);
//
//        tableEnv.executeSql(KafkaTotalAggregateInsert);

// not for stream, only for batch mode
//        final Table finalTable  = result2.union(result3).union(result4).union(result5).union(result6);
      //  finalTable.execute().print();

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
