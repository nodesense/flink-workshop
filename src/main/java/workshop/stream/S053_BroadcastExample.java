package workshop.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class S053_BroadcastExample {

    public static void main(String[] args) throws Exception {

        final List<String> input = new ArrayList<>();
        input.add("region1");
        input.add("region2");
        input.add("region3");
        input.add("region4");

        final List<Tuple2<String, String>> keyedInput = new ArrayList<>();
        keyedInput.add(new Tuple2<>("region1", "user1"));
        keyedInput.add(new Tuple2<>("region1", "user2"));
        keyedInput.add(new Tuple2<>("region2", "user3"));
        keyedInput.add(new Tuple2<>("region2", "user4"));
        keyedInput.add(new Tuple2<>("region3", "user5"));
        keyedInput.add(new Tuple2<>("region3", "user6"));
        keyedInput.add(new Tuple2<>("region4", "user7"));
        keyedInput.add(new Tuple2<>("region4", "user8"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>(
                "Broadcast", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
        );

        KeyedStream<Tuple2<String, String>, String> elementStream = env.fromCollection(keyedInput)
                .rebalance()																			// needed to increase the parallelism
                .map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
                    private static final long serialVersionUID = 8710586935083422712L;

                    @Override
                    public Tuple2<String, String> map(Tuple2<String, String> value) {
                        return value;
                    }
                })
                .setParallelism(4)
                .keyBy(new KeySelector<Tuple2<String, String>, String>() {
                    private static final long serialVersionUID = -1110876099102344900L;

                    @Override
                    public String getKey(Tuple2<String, String> value) {
                        return value.f0;
                    }
                });

        BroadcastStream<String> broadcastStream = env.fromCollection(input)
                .flatMap(new FlatMapFunction<String, String>() {
                    private static final long serialVersionUID = 6462244253439410814L;

                    @Override
                    public void flatMap(String value, Collector<String> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(4)
                .broadcast(mapStateDescriptor);

        DataStream<String> output = elementStream
                .connect(broadcastStream)
                .process(
                        new KeyedBroadcastProcessFunction<String, Tuple2<String, String>, String, String>() {

                            @Override
                            public void processElement(Tuple2<String, String> value, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                                String prev = getRuntimeContext().getState(valueState).value();

                                StringBuilder str = new StringBuilder();
                                str
                                        .append("Value=")
                                        .append(value)
                                        .append(" Broadcast State=[");

                                for (Map.Entry<String, String> entry : ctx.getBroadcastState(localMapStateDescriptor).immutableEntries()) {
                                    str
                                            .append(entry.getKey())
                                            .append("->")
                                            .append(entry.getValue())
                                            .append(" ");
                                }

                                str.append("]");

                                getRuntimeContext().getState(valueState).update(str.toString());

                                out.collect("BEFORE: " + prev + " " + "AFTER: " + str);
                            }

                            @Override
                            public void processBroadcastElement(String value, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, String, String>.Context ctx, Collector<String> out) throws Exception {
                                ctx.getBroadcastState(localMapStateDescriptor).put(value, value);

                                ctx.applyToKeyedState(valueState, new KeyedStateFunction<String, ValueState<String>>() {
                                    @Override
                                    public void process(String key, ValueState<String> state) throws Exception {
                                        out.collect("Broadcast side task#" + getRuntimeContext().getIndexOfThisSubtask() + ": " + key + " " + state.value().toString());
                                    }
                                });
                            }

                            private static final long serialVersionUID = 8512350700250748742L;

                            private final ValueStateDescriptor<String> valueState =
                                    new ValueStateDescriptor<>("any", BasicTypeInfo.STRING_TYPE_INFO);

                            private final MapStateDescriptor<String, String> localMapStateDescriptor =
                                    new MapStateDescriptor<>(
                                            "Broadcast", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
                                    );


                        });
        output.print();
        env.execute();
    }
}