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

/**
 * Example illustrating the use of {@link org.apache.flink.api.common.state.BroadcastState}.
 */
public class S053_BroadcastExample {

    public static void main(String[] args) throws Exception {

        final List<String> input = new ArrayList<>();
        input.add("1");
        input.add("2");
        input.add("3");
        input.add("4");

        final List<Tuple2<Integer, Integer>> keyedInput = new ArrayList<>();
        keyedInput.add(new Tuple2<>(1, 1));
        keyedInput.add(new Tuple2<>(1, 5));
        keyedInput.add(new Tuple2<>(2, 2));
        keyedInput.add(new Tuple2<>(2, 6));
        keyedInput.add(new Tuple2<>(3, 3));
        keyedInput.add(new Tuple2<>(3, 7));
        keyedInput.add(new Tuple2<>(4, 4));
        keyedInput.add(new Tuple2<>(4, 8));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>(
                "Broadcast", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO
        );

        KeyedStream<Tuple2<Integer, Integer>, Integer> elementStream = env.fromCollection(keyedInput)
                .rebalance()																			// needed to increase the parallelism
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    private static final long serialVersionUID = 8710586935083422712L;

                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) {
                        return value;
                    }
                })
                .setParallelism(4)
                .keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
                    private static final long serialVersionUID = -1110876099102344900L;

                    @Override
                    public Integer getKey(Tuple2<Integer, Integer> value) {
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
                        new KeyedBroadcastProcessFunction<String, Tuple2<Integer, Integer>, String, String>() {

                            @Override
                            public void processElement(Tuple2<Integer, Integer> value, KeyedBroadcastProcessFunction<String, Tuple2<Integer, Integer>, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                                String prev = getRuntimeContext().getState(valueState).value();

                                StringBuilder str = new StringBuilder();
                                str
                                        .append("Value=")
                                        .append(value)
                                        .append(" Broadcast State=[");

                                for (Map.Entry<String, Integer> entry : ctx.getBroadcastState(localMapStateDescriptor).immutableEntries()) {
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
                            public void processBroadcastElement(String value, KeyedBroadcastProcessFunction<String, Tuple2<Integer, Integer>, String, String>.Context ctx, Collector<String> out) throws Exception {
                                ctx.getBroadcastState(localMapStateDescriptor).put(value, Integer.valueOf(value));

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

                            private final MapStateDescriptor<String, Integer> localMapStateDescriptor =
                                    new MapStateDescriptor<>(
                                            "Broadcast", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO
                                    );


                        });
        output.print();
        env.execute();
    }
}