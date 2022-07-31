package workshop.analytics;


import workshop.models.TrueDataTick;
import com.google.gson.Gson;
import workshop.models.Candle;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.functions.CandleWindowFunction;

import java.time.Duration;
import java.util.Properties;

public class TrueDataCandleMain2 {
    public static void main(String[] args) throws  Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // EnvironmentSettings env = EnvironmentSettings.inStreamingMode();
        // StreamExecutionEnvironment env

        env.getConfig().setAutoWatermarkInterval(Duration.ofMillis(100).toMillis());
      //  env.getConfig().setLocalTimeZone(ZoneId.of("UTC"));

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);



        Properties kafkaProperties = new Properties();

        kafkaProperties.put("ClassType", TrueDataTick.class);
        kafkaProperties.put("kafka.topic", "nse-live-ticks3");
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers((String) kafkaProperties.get("bootstrap.servers"))
                .setTopics("nse-live-ticks3")
                .setGroupId("my-groupewqewq6")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> strStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

 //       strStream.print();

        DataStream<TrueDataTick> dataStream = strStream.map(json ->    new Gson().fromJson(json, TrueDataTick.class));

        //dataStream.print();
                WatermarkStrategy<TrueDataTick> watermarkStrategy =  WatermarkStrategy
                .<TrueDataTick>forBoundedOutOfOrderness(Duration.ofSeconds(2))
               // .forMonotonousTimestamps()
             .withTimestampAssigner((event, timestamp) -> event.Timestamp.getTime() + 330 * 60 * 1000);


        DataStream<TrueDataTick>  dataValueStreamWithWaterMark = dataStream
                .assignTimestampsAndWatermarks(watermarkStrategy);


        SingleOutputStreamOperator<Candle> candleStream = dataValueStreamWithWaterMark
                .keyBy( (tick) -> tick.Symbol )
                //  .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(1))) // results producerd  1 sec offset past time interval like 11 instead of 10
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // rounded to wall clock 5, 10, 15, ...
                // .process(new TAWindow("SMA", 5))
                .process(new CandleWindowFunction("OohMyName.Fun"))
                ;

        candleStream.print();

        env.execute();
    }
}
