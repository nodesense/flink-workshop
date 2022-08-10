package workshop.stream;


import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class S054_WindowJoinUserRegionPageViews {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize", 2000);
        final long rate = params.getLong("rate", 3L);

        System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);
        System.out.println(
                "To customize example, use: WindowJoin [--windowSize <window-size-in-millis>] [--rate <elements-per-second>]");

        // obtain execution environment, run this example in "ingestion time"
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // create the data sources for both grades and salaries
        DataStream<Tuple2<String, String>> users =
                S054_PageVisitSimulation.UserSource.getSource(env, rate)
                        .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<Tuple2<String, String>> pageViews =
                S054_PageVisitSimulation.PageSource.getSource(env, rate)
                        .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        // run the actual window join program
        // for testability, this functionality is in a separate method.
        DataStream<Tuple3<String, String, String>> joinedStream =
                runWindowJoin(users, pageViews, windowSize);

        // print the results with a single thread, rather than in parallel
        joinedStream.print().setParallelism(1);

        // execute program
        env.execute("Windowed Join Example");
    }

    public static DataStream<Tuple3<String, String, String>> runWindowJoin(
            DataStream<Tuple2<String, String>> users,
            DataStream<Tuple2<String, String>> pageViews,
            long windowSize) {

        return users.join(pageViews)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(
                        new JoinFunction<
                                Tuple2<String, String>,
                                Tuple2<String, String>,
                                Tuple3<String, String, String>>() {

                            @Override
                            public Tuple3<String, String, String> join(
                                    Tuple2<String, String> first, Tuple2<String, String> second) {
                                return new Tuple3<String, String, String>(
                                        first.f0, first.f1, second.f1);
                            }
                        });
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, String>, String> {
        @Override
        public String getKey(Tuple2<String, String> value) {
            return value.f0;
        }
    }

    /**
     * This {@link WatermarkStrategy} assigns the current system time as the event-time timestamp.
     * In a real use case you should use proper timestamps and an appropriate {@link
     * WatermarkStrategy}.
     */
    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}