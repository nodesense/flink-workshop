package workshop.functions;

import workshop.models.Tick;
import workshop.models.TickSummary;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class TickAnalysisWindowFunction extends  ProcessWindowFunction<Tick, TickSummary, String, TimeWindow> {

    private ValueStateDescriptor<TickSummary> candleStateDescriptor = new ValueStateDescriptor("CandleState",TickSummary.class);
    private ValueStateDescriptor<Tick> lastTickStateDescriptor = new ValueStateDescriptor("LastTickState",Tick.class);



    public TickAnalysisWindowFunction(String name) {

    }

    private static final long serialVersionUID = 1L;



    @Override
    public void process(String key, Context context, Iterable<Tick> iterable, Collector<TickSummary> collector) throws Exception {

        // System.out.println("Iterator Begin " + new Date());

        // Context.windowState is per window level, keyed, within window like thumling window
        ValueState<TickSummary> candleState = context.windowState().getState(candleStateDescriptor);
        // globalState is global level, still keyed
        ValueState<Tick> lastTickState = context.globalState().getState(lastTickStateDescriptor);

        TickSummary tickSummary = candleState.value();

        Tick lastTick = null;
        Tick firstTick = null;


        for(Tick tick : iterable){
            if (firstTick == null) {
                firstTick = tick;
                lastTick = tick;
            }

            lastTick = tick;



            lastTick = tick;
        }


        if (firstTick != null && lastTick != null) {
            double gain = lastTick.LTP - firstTick.LTP;

            if ((gain >= 1.8) && (gain <= 2.2)) {
                TickSummary result = new TickSummary();
                result.asset = lastTick.asset;
                result.ts = lastTick.ts;
                result.gain = gain;
                collector.collect((result));
            }
        }

    }
}