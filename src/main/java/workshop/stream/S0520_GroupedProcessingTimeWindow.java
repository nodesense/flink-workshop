package workshop.stream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;


public class S0520_GroupedProcessingTimeWindow {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/output/checkpoints/sourcestate/example");


        DataStream<Tuple2<Long, Long>> stream = env.addSource(new DataSource());

        stream.keyBy(value -> value.f0)
                .window(
                        SlidingProcessingTimeWindows.of(
                                Time.milliseconds(2500), Time.milliseconds(500)))
                .reduce(new SummingReducer())

                // alternative: use a apply function which does not pre-aggregate
                //			.keyBy(new FirstFieldKeyExtractor<Tuple2<Long, Long>, Long>())
                //			.window(SlidingProcessingTimeWindows.of(Time.milliseconds(2500),
                // Time.milliseconds(500)))
                //			.apply(new SummingWindowFunction())

                .addSink(new DiscardingSink<>());

        env.execute();
    }

    private static class FirstFieldKeyExtractor<Type extends Tuple, Key>
            implements KeySelector<Type, Key> {

        @Override
        @SuppressWarnings("unchecked")
        public Key getKey(Type value) {
            return (Key) value.getField(0);
        }
    }

    private static class SummingWindowFunction
            implements WindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long, Window> {

        @Override
        public void apply(
                Long key,
                Window window,
                Iterable<Tuple2<Long, Long>> values,
                Collector<Tuple2<Long, Long>> out) {
            long sum = 0L;
            for (Tuple2<Long, Long> value : values) {
                sum += value.f1;
            }

            out.collect(new Tuple2<>(key, sum));
        }
    }

    private static class SummingReducer implements ReduceFunction<Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }

    /**
     * Parallel data source that serves a list of key-value pairs.
     */
    private static class DataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> implements CheckpointedFunction {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

            final long startTime = System.currentTimeMillis();

            final long numElements = 20000000;
            final long numKeys = 10000;
            long val = 1L;
            long count = 0L;

            while (running && count < numElements) {
                count++;
                ctx.collect(new Tuple2<>(val++, 1L));

                if (val > numKeys) {
                    val = 1L;
                }
            }

            final long endTime = System.currentTimeMillis();
            System.out.println(
                    "Took " + (endTime - startTime) + " msecs for " + numElements + " values");
        }

        @Override
        public void cancel() {
            running = false;
        }

        // Whenever a checkpoint has to be performed, snapshotState() is called.
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            System.out.println("snapstate called ");
        }

        //The counterpart, initializeState(), is called every time the user-defined function is initialized
        //  be that when the function is first initialized or
        //  be that when the function is actually recovering from an earlier checkpoint.
        //  Given this, initializeState() is not only the place where different types of state are initialized,
        //  but also where state recovery logic is included.

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            System.out.println("initializeState called ");
        }
    }
}