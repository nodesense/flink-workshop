package workshop.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.ta4j.core.*;
import org.ta4j.core.indicators.*;
import org.ta4j.core.indicators.adx.MinusDIIndicator;
import org.ta4j.core.indicators.adx.PlusDIIndicator;
import org.ta4j.core.indicators.bollinger.BollingerBandsLowerIndicator;
import org.ta4j.core.indicators.bollinger.BollingerBandsMiddleIndicator;
import org.ta4j.core.indicators.bollinger.BollingerBandsUpperIndicator;
import org.ta4j.core.indicators.helpers.*;
import org.ta4j.core.indicators.statistics.MeanDeviationIndicator;
import org.ta4j.core.indicators.statistics.SimpleLinearRegressionIndicator;
import org.ta4j.core.indicators.statistics.StandardDeviationIndicator;
import org.ta4j.core.num.DecimalNum;
import org.ta4j.core.num.Num;
import org.ta4j.core.rules.OverIndicatorRule;
import org.ta4j.core.rules.UnderIndicatorRule;
import workshop.models.Tick;
import workshop.models.TickTA;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class TickRainbowAnalysisWindow extends KeyedProcessFunction<String, Tick, TickTA> {

    public static enum TrailMode { None , ActiveLow, ActiveMid , ActiveHigh  }

    public static class TradeInProfileIndicator extends CachedIndicator<Num> {
        TrailMode trailMode = TrailMode.None;

        Num stopLossDistance = DecimalNum.valueOf(0);
        Num stopLossLimit = DecimalNum.valueOf(0);
        public boolean inTrade = false;
        public  int enterIndex = -1;


        public  void reset() {
            inTrade = false;
            enterIndex = -1;
            trailMode = TrailMode.None;
        }
        private final Indicator<Num> indicator;


        public TradeInProfileIndicator(Indicator<Num> indicator) {
            super(indicator);
            this.indicator = indicator;
        }

        @Override
        protected Num calculate(int index) {
             if (!inTrade) {
                 return DecimalNum.valueOf(0);
             }

             Num startValue = indicator.getValue(enterIndex);
             Num currentValue = indicator.getValue(index);
             Num diff = currentValue.minus(startValue);

             switch (trailMode) {
                 case None: {
                      if (diff.isGreaterThanOrEqual(DecimalNum.valueOf(6))) {
                         stopLossDistance = DecimalNum.valueOf(2);
                         trailMode = TrailMode.ActiveHigh;
                         stopLossLimit =  currentValue.minus(stopLossDistance).minus(DecimalNum.valueOf(0.1));
                     }
                 }
                 break;
             }


            if (trailMode == TrailMode.None) {
                // TODO: is loss too early in trade, then kill the trade

                return DecimalNum.valueOf(0);
            }

            Num referenceValue = stopLossLimit.plus(stopLossDistance);

            if (currentValue.isGreaterThan(referenceValue)) {

                Num oS = stopLossLimit;
                stopLossLimit = currentValue.minus(stopLossDistance);

                System.out.println("TRAILING " + trailMode + " LIMIT " +   stopLossLimit  + " FROM " + oS);

                return DecimalNum.valueOf(0);
                // happy, return 0 a swe are in profit
            }

            if (currentValue.isLessThan(stopLossLimit)) {
                System.out.println("Final Trial output  start "  +  startValue + " STOP " +  stopLossLimit + " current Value " +   currentValue + " GAIN " + currentValue.minus(startValue));

                return DecimalNum.valueOf(1);
            }

           // if (diff.isGreaterThanOrEqual(DecimalNum.valueOf(8)))  return DecimalNum.valueOf(1);
            // challenge part.. mostly bye now..
            System.out.println("IN  Trial output  start "  +  startValue + " STOP " +  stopLossLimit + " current Value " +   currentValue + " GAIN " + currentValue.minus(startValue));


            return DecimalNum.valueOf(0);

        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + " barCount: " ;
        }
    }

    ClosePriceIndicator closePrice = null;
    ClosePriceIndicator lrClosePrice = null;



    BarSeries series;

    BarSeries lrSeries;
    ValueState<Double> callSignalState;
    private ValueStateDescriptor<Double> callSignalStateDescriptor = new ValueStateDescriptor("CAllSignal",Double.class);

    ValueState<Integer> callEntryIndexState;
    private ValueStateDescriptor<Integer> callEntryIndexStateDescriptor = new ValueStateDescriptor("callEntryIndexState",Integer.class);


    ValueState<Double> putSignalState;
    private ValueStateDescriptor<Double> putSingalStateDescriptor = new ValueStateDescriptor("PutSignal",Double.class);

    ValueState<Integer> putEntryIndexState;
    private ValueStateDescriptor<Integer> putEntryIndexStateDescriptor = new ValueStateDescriptor("putEntryIndexState",Integer.class);


    ValueStateDescriptor<BarSeries> barSeriesState;
    String name;


    TradeInProfileIndicator tradeInProfit = null;
    int barLength = 30;
    public TickRainbowAnalysisWindow(String name, int barLength) {
        this.name = name;
        this.barLength = barLength;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        barSeriesState   = new ValueStateDescriptor<>("barSeriesState", BarSeries.class);
        build();
    }

    Map<String, Indicator<Num>> indicatorMap = new HashMap<>();

    private static final long serialVersionUID = 1L;


    Indicator<Num> lr = null;


    Indicator<Num> lwma;
    Indicator rsi;

    Indicator bollingerUp = null;
    Indicator bollingerLower = null;

    Indicator<Num> getIndicator(String name) {

        return indicatorMap.getOrDefault(name, null);

    }

    void build() {
        series = new BaseBarSeriesBuilder().withMaxBarCount(1000000).withName("my_2017_series").build();
        lrSeries= new BaseBarSeriesBuilder().withMaxBarCount(1000000).withName("my_2018_series").build();

        closePrice = new ClosePriceIndicator(series);
        lrClosePrice = new ClosePriceIndicator(lrSeries);

        lr = new SimpleLinearRegressionIndicator(closePrice, 5, SimpleLinearRegressionIndicator.SimpleLinearRegressionType.Y);
        indicatorMap.put("lr",  lr);

        for (int i = 0; i < 24; i++) {
            Indicator<Num> indicator =  new KAMAIndicator(lrClosePrice, 10 + i, 2, 30 + i * 3);
            indicatorMap.put("kama" + i,  indicator);
        }

        indicatorMap.put("pdi", new PlusDIIndicator(lrSeries,  barLength));
        indicatorMap.put("mdi", new MinusDIIndicator(lrSeries,  barLength));

        lwma = new LWMAIndicator(lrClosePrice, 10  * barLength);
        indicatorMap.put("lwma",  lwma);
        rsi = new RSIIndicator(lrClosePrice, 5 * barLength);
        indicatorMap.put("rsi",  rsi);

        BollingerBandsMiddleIndicator bollingerMiddle = new BollingerBandsMiddleIndicator(lrClosePrice);
        bollingerUp = new BollingerBandsUpperIndicator(bollingerMiddle, lrClosePrice, DecimalNum.valueOf(5 * barLength));
        Indicator bollingerLower = new BollingerBandsLowerIndicator(bollingerMiddle, lrClosePrice, DecimalNum.valueOf(5 * barLength));

        indicatorMap.put("bolup", bollingerUp);
        indicatorMap.put("bollow", bollingerLower);
        indicatorMap.put("bolmid", bollingerMiddle);
        indicatorMap.put("lwma", lwma);

        indicatorMap.put("roc", new ROCIndicator(lr, 15));
        indicatorMap.put("roc10", new ROCIndicator(lr, 10));
        indicatorMap.put("roc20", new ROCIndicator(lr, 20));
        indicatorMap.put("roc30", new ROCIndicator(lr, 30));
        indicatorMap.put("roc60", new ROCIndicator(lr, 60));

        indicatorMap.put("low10", new LowestValueIndicator(lr, 10));
        indicatorMap.put("low20", new LowestValueIndicator(lr, 20));
        indicatorMap.put("low30", new LowestValueIndicator(lr, 30));
        indicatorMap.put("low60", new LowestValueIndicator(lr, 60));


        indicatorMap.put("high10", new HighestValueIndicator(lr, 10));
        indicatorMap.put("high20", new HighestValueIndicator(lr, 20));
        indicatorMap.put("high30", new HighestValueIndicator(lr, 30));
        indicatorMap.put("high60", new HighestValueIndicator(lr, 60));

        tradeInProfit = new TradeInProfileIndicator(closePrice);

        Rule callEnterRule =   new OverIndicatorRule(indicatorMap.get("kama0"), lr)
                .and(new OverIndicatorRule(lr, lwma));

        for (int i = 0 ; i < 23; i++) {
            callEnterRule = callEnterRule.and(new OverIndicatorRule(indicatorMap.get("kama" + i), indicatorMap.get("kama" + (i + 1))));
        }

        Rule callExitRule =    new OverIndicatorRule(lr, indicatorMap.get("kama0"));
        for (int i = 23 ; i > 0; i++) {
            callExitRule = callExitRule.and(new OverIndicatorRule(indicatorMap.get("kama" + (i + 23)), indicatorMap.get("kama" + (i - 1))));
        }

        callStrategy = new BaseStrategy(callEnterRule, callExitRule);
        putStrategy = new BaseStrategy(callExitRule, callEnterRule);
    }

    Strategy callStrategy;
    Strategy putStrategy;

    Num getValue(String name, int index ) {
        Indicator<Num> indicator = indicatorMap.get(name);
        return indicator.getValue(index);
    }

    boolean specialCallExit = false;

    Num stopLossLimit = DecimalNum.valueOf(-1);


    @Override
    public void processElement(
            Tick tick ,
            Context context,
            Collector<TickTA> collector) throws Exception {


        callSignalState = getRuntimeContext().getState(callSignalStateDescriptor);

        if (callSignalState.value() == null) {
            callSignalState.update(0.0); // 0 - neutral, no signal or nothing
        }

        callEntryIndexState = getRuntimeContext().getState(callEntryIndexStateDescriptor);

        if (callEntryIndexState.value() == null) {
            callEntryIndexState.update(-1); // 0 - neutral, no signal or nothing
        }


        double callSignal = callSignalState.value();

        //--
        putSignalState = getRuntimeContext().getState(putSingalStateDescriptor);

        if (putSignalState.value() == null) {
            putSignalState.update(0.0); // 0 - neutral, no signal or nothing
        }

        putEntryIndexState = getRuntimeContext().getState(putEntryIndexStateDescriptor);

        if (putEntryIndexState.value() == null) {
            putEntryIndexState.update(-1); // 0 - neutral, no signal or nothing
        }


        double putSignal = putSignalState.value();


        ZonedDateTime d = ZonedDateTime.ofInstant(tick.ts.toInstant(), ZoneId.of("UTC"));
        try {
            series.addBar(d, tick.LTP, tick.LTP, tick.LTP, tick.LTP);
        }
        catch (IllegalArgumentException iae) {
         //   System.out.println("ERROR " + iae.getMessage());
        }
        catch(Exception e) {
            throw e;
        }


        try {
            Num lrValue = closePrice.getValue(series.getEndIndex());
            lrSeries.addBar(d, lrValue.doubleValue(), lrValue.doubleValue(), lrValue.doubleValue(), lrValue.doubleValue());
        }
        catch (IllegalArgumentException iae) {
            //   System.out.println("ERROR " + iae.getMessage());
        }
        catch(Exception e) {
            throw e;
        }


        int index = lrSeries.getEndIndex();

        if (index < (200)) return;

       // if (index < support) return;

   //     System.out.println("Index " + index +   " Removed count " +  series.getRemovedBarsCount() +  " Bar BCount " +  series.getBarCount());
        TickTA taResult = new TickTA();

        taResult.asset = tick.asset;
        taResult.ts = tick.ts;

        for (String name: indicatorMap.keySet()) {
            Num n = getValue(name, index);

            if (n.isNaN()) {
                taResult.fields.put(name, -1.0);
            } else {
                taResult.fields.put(name, n.doubleValue());
            }
        }

        taResult.fields.put("LTP", tick.LTP);


        boolean callEnter  = callStrategy.shouldEnter(index);

        if (callEnter) {
            if (callSignal == 0 || callSignal == -1.0) {
                taResult.fields.put("enterCall", 1.0);
                callSignal = 1.0;
                callSignalState.update(callSignal);
                callEntryIndexState.update(lrSeries.getEndIndex());
                specialCallExit = true;
                stopLossLimit = lrClosePrice.getValue(lrSeries.getEndIndex());
                tradeInProfit.reset();
                tradeInProfit.inTrade = true;
                tradeInProfit.enterIndex = lrSeries.getEndIndex();
                taResult.fields.put("callPrice", lrClosePrice.getValue(lrSeries.getEndIndex()).doubleValue());
            }
        }

        if ((callSignal == 1.0) && specialCallExit) {
            Num startValue = lrClosePrice.getValue(callEntryIndexState.value());
            Num currentValue = lrClosePrice.getValue(lrSeries.getEndIndex());
            if (currentValue.minus(startValue).isGreaterThanOrEqual(DecimalNum.valueOf(8))) {
                taResult.fields.put("specialCallExit", 1.0);
                specialCallExit = false;
            }
        }

        if ((callSignal == 1.0)) {
            Num startValue = lrClosePrice.getValue(callEntryIndexState.value());
            Num currentValue = lrClosePrice.getValue(lrSeries.getEndIndex());

            Num stopLossDistance = DecimalNum.valueOf(0.5);

            Num referenceValue = stopLossLimit.plus(stopLossDistance);

            if (currentValue.isGreaterThan(referenceValue)) {
                stopLossLimit = currentValue.minus(stopLossDistance);
            }
        }



        boolean callExit  = callStrategy.shouldExit(index);
        if (callExit) {
            if (callSignal == 1.0) {
                taResult.fields.put("exitCall", 1.0);
                callSignal = -1;
                callSignalState.update(callSignal);

                int startIndex = callEntryIndexState.value();
                callEntryIndexState.update(-1);
                double startLTP = lrClosePrice.getValue(startIndex).doubleValue();
                double currentValue = lrClosePrice.getValue(lrSeries.getEndIndex()).doubleValue();
                taResult.fields.put("callProfit", currentValue - startLTP);

                Bar startBar = lrSeries.getBar(startIndex);
                Bar endBar = lrSeries.getBar(index);
                taResult.fields.put("durationSeconds", Double.valueOf(index - startIndex));
                taResult.fields.put("stopLossLimit", stopLossLimit.doubleValue() - currentValue);


                taResult.fields.put("sellPrice", lrClosePrice.getValue(lrSeries.getEndIndex()).doubleValue());

                tradeInProfit.reset();
                tradeInProfit.inTrade = false;
                tradeInProfit.enterIndex = -1;

            }
        }

        boolean putEnter  = putStrategy.shouldEnter(index);

        if (putEnter) {
            if (putSignal == 0 || putSignal == -1.0) {
                taResult.fields.put("enterPut", 1.0);
                putSignal = 1.0;
                putSignalState.update(putSignal);
                putEntryIndexState.update(lrSeries.getEndIndex());
            }
        }

        boolean putExit = putStrategy.shouldExit(index);

        if (putExit)
        {
            if (putSignal == 1.0) {
                taResult.fields.put("exitPut", 1.0);
                putSignal = -1;
                putSignalState.update(putSignal);

                int startIndex = putEntryIndexState.value();
                putEntryIndexState.update(-1);
                double startLTP = lrClosePrice.getValue(startIndex).doubleValue();
                double currentValue = lrClosePrice.getValue(lrSeries.getEndIndex()).doubleValue();
                taResult.fields.put("putProfit",   startLTP - currentValue);
                taResult.fields.put("durationSeconds", Double.valueOf(index - startIndex));
            }
        }


        collector.collect(taResult);
    }

}