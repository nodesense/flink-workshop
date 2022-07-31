package workshop.sources;

import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Properties;

public class  JsonKafkaSource<T> {
    protected StreamExecutionEnvironment env = null;
    protected StreamTableEnvironment tableEnv = null;

    public JsonKafkaSource( StreamExecutionEnvironment env,  StreamTableEnvironment tableEnv ) {
        this.env = env;
        this.tableEnv = tableEnv;
    }

    public  DataStream<T> load(Properties properties) throws  Exception {
        Class classType = (Class) properties.get("ClassType");
        Class<T> clazz = classType;
        String topic = (String) properties.get("kafka.topic");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers((String) properties.get("bootstrap.servers"))
                .setTopics(topic)
                .setGroupId("my-groupewqewq6")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();



       // DataStreamSource<String> strStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
                        DataStreamSource<String> strStream = env.fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)),
                                                        "Kafka Source");

        strStream.print();

        DataStream<T> dataStream = strStream.map(json ->   clazz.cast( new Gson().fromJson(json, classType))).returns(classType);


        return dataStream;
    }

}
