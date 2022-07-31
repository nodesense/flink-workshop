package workshop.stream;


    import org.apache.flink.api.common.functions.FlatMapFunction;
    import org.apache.flink.streaming.api.datastream.DataStream;
    import org.apache.flink.api.java.tuple.Tuple2;
    import org.apache.flink.util.Collector;
    import org.apache.flink.connector.kafka.source.KafkaSource;
    import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
    import org.apache.flink.connector.kafka.sink.KafkaSink;
    import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
    import org.apache.flink.api.common.serialization.SimpleStringSchema;
    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
    import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * This is a re-write of the Apache Flink WordCount example using Kafka connectors.
 * Find the original example at
 * https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/wordcount/WordCount.java
 */
public class S044_KafkaWordCount {

    //     kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic words
    // kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic word-counts
    //
    // kafka-console-producer --broker-list localhost:9092 --topic words
    // kafka-console-consumer --bootstrap-server localhost:9092 --topic word-counts

    final static String inputTopic = "words";
    final static String outputTopic = "word-counts";
    final static String jobTitle = "WordCount";

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(new SimpleStringSchema())
                .setTopic(outputTopic)
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(serializer)
                .build();

        DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


        // Split up the lines in pairs (2-tuples) containing: (word,1)
        DataStream<String> counts = text.flatMap(new Tokenizer())
                // Group by the tuple field "0" and sum up tuple field "1"
                .keyBy(value -> value.f0)
                .sum(1)
                .flatMap(new Reducer());

        // Add the sink to so results
        // are written to the outputTopic
        counts.sinkTo(sink);

        // Execute program
        env.execute(jobTitle);
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // Emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    // Implements a simple reducer using FlatMap to
    // reduce the Tuple2 into a single string for
    // writing to kafka topics
    public static final class Reducer
            implements FlatMapFunction<Tuple2<String, Integer>, String> {

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<String> out) {
            // Convert the pairs to a string
            // for easy writing to Kafka Topic
            String count = value.f0 + " " + value.f1;
            out.collect(count);
        }
    }
}