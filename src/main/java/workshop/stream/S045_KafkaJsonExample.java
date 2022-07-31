package workshop.stream;

import com.google.gson.Gson;
import workshop.models.OrderPOJO;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.*;

//     kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic order-input
// kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic order-output
//
// kafka-console-producer --broker-list localhost:9092 --topic order-input
// kafka-console-consumer --bootstrap-server localhost:9092 --topic order-output

// {"regionId": "north1", "pageId": "book1"}

public class S045_KafkaJsonExample {
    static java.util.logging.Logger logger =  java.util.logging.Logger.getLogger(String.valueOf(S045_KafkaJsonExample.class));

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        Properties properties = new Properties();

//        properties.setProperty("bootstrap.servers", "kafka-cluster-headless:9092");
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "order-group-demo");

//        properties.setProperty("bootstrap.servers", "localhost:9092");

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer("order-input", new SimpleStringSchema(), properties));

        DataStream<OrderPOJO> dataStream = stream
                .map(json ->  new Gson().fromJson(json, OrderPOJO.class)
                );
                //.assignTimestampsAndWatermarks(new DataValueTimeAssigner());


        dataStream.print();


        DataStream<String> texts = dataStream.map( dataValue -> new Gson().toJson(dataValue, OrderPOJO.class));
        texts.print();


        Properties sinkproperties = new Properties();

        sinkproperties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "order-output",                  // target topic
                new org.apache.flink.api.common.serialization.SimpleStringSchema(),    // serialization schema
                sinkproperties                 // producer config
        );
        kafkaProducer.setWriteTimestampToKafka(true);

        texts.addSink(kafkaProducer);

        env.execute();

    }
    }

