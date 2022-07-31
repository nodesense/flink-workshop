package workshop.analytics;


import workshop.functions.TickAnalysisWindow;
import workshop.models.Tick;
import workshop.models.TickTA;
import workshop.sources.JsonFileSource;
import workshop.encoders.SimpleJsonEncoder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.StdDateFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

//import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
//import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
//import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;
import workshop.functions.*;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TickAnalysisMain {




    static public class FileSink {
        protected  StreamExecutionEnvironment env = null;
        protected  StreamTableEnvironment tableEnv = null;

        public FileSink( StreamExecutionEnvironment env,  StreamTableEnvironment tableEnv ) {
            this.env = env;
            this.tableEnv = tableEnv;
        }
    }

    static public class UsageRecordSerializationSchema implements KafkaRecordSerializationSchema<TickTA> {

        private static final long serialVersionUID = 1L;

        private String topic;
        private final ObjectMapper objectMapper =
                JsonMapper.builder()
                        .build();
                       // .registerModule(new JavaTimeModule())
                       // .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);


    public UsageRecordSerializationSchema() {
        // 2022-07-07T09:16:45
        //
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-ddThh:mm:ss");
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
        // df.setTimeZone(TimeZone.getTimeZone("UTC"));
        //objectMapper.setDateFormat(df);
    }

    public UsageRecordSerializationSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                TickTA element, KafkaSinkContext context, Long timestamp) {

            try {
                return new ProducerRecord<>(
                        topic,
                        null,
                        element.ts.getTime(),
                        element.asset.getBytes(StandardCharsets.UTF_8),
                        objectMapper.writeValueAsBytes(element));
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Could not serialize record: " + element, e);
            }
        }
    }

    static class TickAnalysis {
        protected  StreamExecutionEnvironment env = null;
        protected  StreamTableEnvironment tableEnv = null;
        protected  Properties properties = null;
        public void config(Properties properties) {
            this.properties = properties;
            // set up the Java DataStream API
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            // set up the Java Table API
            tableEnv = StreamTableEnvironment.create(env);
        }

        public void process () throws  Exception  {
            //tickDataStream.print();

            Properties properties = new Properties();

            properties.put("ClassType", Tick.class);
            properties.put("kafka.topic", "nse_ticks_test2");

            JsonFileSource<Tick> tickJsonSource = new JsonFileSource<Tick>(env, tableEnv);
            DataStream<Tick>   tickDataStream = tickJsonSource.load (properties);



            // tickDataStream.print();

//            KafkaSourceTemp<Tick> kafkaSource = new KafkaSourceTemp<>(env, tableEnv);
//            properties.setProperty("bootstrap.servers", "localhost:9092");
//
//           DataStream<Tick> tickDataStream = kafkaSource.load(properties);
//            // Long??
            WatermarkStrategy<Tick> watermarkStrategy =  WatermarkStrategy
                    .<Tick>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((event, timestamp) -> event.ts.getTime());


//            DataStream<Tick>  dataValueStreamWithWaterMark = tickDataStream
//                    .assignTimestampsAndWatermarks(watermarkStrategy);


            final StreamingFileSink<TickTA> fileSink = StreamingFileSink
                    .forRowFormat(new Path("/home/krish/IdeaProjects/FlinkDemo/outputs"), new SimpleJsonEncoder<TickTA>())
                    .withRollingPolicy(
                            DefaultRollingPolicy.builder()
                                    .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
                                    .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                    .withMaxPartSize(1024 * 1024 * 1024)
                                    .build())
                    .build();


            SingleOutputStreamOperator<TickTA> tickTAStream = tickDataStream // dataValueStreamWithWaterMark
                    .filter( tick -> tick.asset.equals("NIFTY 50"))
                    .keyBy( (tick) -> tick.asset )
                    // .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                    // .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                    // .process(new TAWindow("SMA", 5))
                    .process(new TickAnalysisWindow("OohMyName.Fun", 30))
                    ;

            tickTAStream.addSink(fileSink);

            PrintSinkFunction<TickTA> printFunction = new PrintSinkFunction<>();

//            tickTAStream
//                    .filter( tickTA -> ( tickTA.getField("specialCallExit") == 1  || tickTA.getField("enterCall") == 1) || (tickTA.getField("exitCall") == 1) || (tickTA.getField("enterPut") == 1) || (tickTA.getField("exitPut") == 1) )
//                    .addSink(printFunction);


            Properties sinkproperties = new Properties();

            sinkproperties.setProperty("bootstrap.servers", "localhost:9092");


            KafkaSink<TickTA> kafkaSink = KafkaSink.<TickTA>builder()
                    .setBootstrapServers("localhost:9092")
                    .setRecordSerializer(new UsageRecordSerializationSchema("ticks-ta2"))
                    .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            tickTAStream
                    //.filter( tickTA -> (tickTA.getField("enterCall") == 1) || (tickTA.getField("exitCall") == 1) || (tickTA.getField("enterPut") == 1) || (tickTA.getField("exitPut") == 1) )
                    .sinkTo(kafkaSink);

            Table resultTable = tableEnv.fromDataStream(tickTAStream);

            env.execute();

        }
    }

    public static void main(String[] args) throws  Exception {
        TickAnalysis ta = new TickAnalysis();
        Properties properties = new Properties();
        ta.config(properties);
        ta.process();

//
//        //InfluxDBConfig influxDBConfig = new InfluxDBConfig.Builder("http://localhost:8086", "root", "root", "db_flink_test")
//        InfluxDBConfig influxDBConfig = InfluxDBConfig.builder("http://localhost:8086", "myusername", "passwordpasswordpassword", "TickAnalysis30")
//                .batchActions(1000)
//                .flushDuration(100, TimeUnit.MILLISECONDS)
//                .enableGzip(true)
//                .build();

//
//        DataStream<InfluxDBPoint> influxDataStream = tickTAStream.map(new TickToInfluxPointFunction());
//
//        influxDataStream.addSink(new InfluxDBSink(influxDBConfig));

    }
}
