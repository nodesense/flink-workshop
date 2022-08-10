package workshop.functions;

import workshop.models.Candle;
import workshop.models.TrueDataTick;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;

// TrueDataTick is input,
// second arg Candle is output type
// 3rd arg is Key, string type,
// TimeWindow is from tumble, session or sliding window
public class TickWindowDemo extends  ProcessWindowFunction<TrueDataTick, Candle, String, TimeWindow> {
    // state, complex business logic which cannot done in sql etc
    private ValueStateDescriptor<TrueDataTick> lastTickStateDescriptor = new ValueStateDescriptor("LastTickState",TrueDataTick.class);
    private ValueStateDescriptor<Candle> candleValueStateDescriptor = new ValueStateDescriptor("CandleState",Candle.class);

    public TickWindowDemo() {
        System.out.println("TickWindowDemo created " );
    }

    // Keyed state , keyby (tick -> tick.Symbol)
    // Window Scope with in keyed state: Flink support Window level state [Thumbling , Sliding window]
    // a window state cannot be used in another window state
    // Global Scope within keyed state, Flink support global state
    // global state can be used by any window
    @Override
    public void process(String key, // keyBy
                        Context context, // help us to get state, global state, window state
                        Iterable<TrueDataTick> iterable, // input events from stream
                        Collector<Candle> collector // output data
    ) throws Exception {
        System.out.println("---------------------------");
        System.out.println("KEY: " + key);


        // context helps to get the state
        // context.windowState() - helps to get current window state
        // window state is destroed when window lifecycle is over
        //  context.globalState() - helps to get global state data
        //  globalState is maintained throughout pipeline execution

        ValueState<TrueDataTick> lastTickState = context.globalState().getState(lastTickStateDescriptor);

        ValueState<Candle> candleValueState = context.windowState().getState(candleValueStateDescriptor);

        // useful  if the pipeline crashed and restored on another system
        // otherwise it retursn null
        Candle candle = candleValueState.value();

        // For volume calculation,
        //   we need last tick , then we need current tick
        //    Volume Traded in that second/sub sequent event = current.Volume - lastTick.Volume
        // volume traded in that minute
        //    Volume Traded += current.Volume - lastTick.Volume

        // same way, we will calculated TA, Traded Amount
        // last tick is picked from global, it means, that we get last tick data from previous window


        // if there is no previouse window, lastTickState.value() to get state shal return null
        TrueDataTick lastTick = lastTickState.value(); // get last tick from previous window or null
        System.out.println("From Previuos window last tick " + lastTick);

        for (TrueDataTick tick: iterable) {
            System.out.println(tick);
            if (candle == null) {

                candle = new Candle();
                candle.asset = tick.Symbol;
                candle.st = tick.Timestamp;
                candle.O = tick.LTP; // last traded stock price
                candle.L = tick.LTP;
                candle.H = tick.LTP;
                candle.C = tick.LTP;
                candle.V = 0.0;
            }

            candle.C  = tick.LTP;
            candle.H = Math.max(candle.H, tick.LTP);
            candle.L = Math.min(candle.L, tick.LTP);
            candle.et = tick.Timestamp;
            // how to get the last tick
            if (lastTick != null) {
                //    Volume Traded += current.Volume - lastTick.Volume
                candle.V += tick.Volume - lastTick.Volume;
            }

            lastTick = tick; // this last one to be set to global state
            candleValueState.update(candle); // do here if candle calculate is heavy CPU compution
        }

        if (lastTick != null) {
            // update global state so that next window shall get last tick from this window
            lastTickState.update(lastTick);
        }

        if (candle != null) {
            candle.A = (candle.C + candle.H + candle.L) / 3.0;
            // generate output that goes to flink stream
            collector.collect(candle); // this candle shall be produced and send to sink/downstream
        }
        System.out.println("---------------------------");
    }
}