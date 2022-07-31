package workshop.functions;

import workshop.models.Tick;
import workshop.models.TickTA;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.ta4j.core.*;
import org.ta4j.core.indicators.*;
import org.ta4j.core.indicators.adx.PlusDIIndicator;

import org.ta4j.core.indicators.adx.MinusDIIndicator;
import org.ta4j.core.indicators.bollinger.BollingerBandsLowerIndicator;
import org.ta4j.core.indicators.bollinger.BollingerBandsMiddleIndicator;
import org.ta4j.core.indicators.bollinger.BollingerBandsUpperIndicator;
import org.ta4j.core.indicators.helpers.*;
import org.ta4j.core.indicators.statistics.*;
import org.ta4j.core.num.DecimalNum;
import org.ta4j.core.num.Num;
import org.ta4j.core.rules.OverIndicatorRule;
import org.ta4j.core.rules.UnderIndicatorRule;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class TickAnalysisWindow extends KeyedProcessFunction<String, Tick, TickTA> {

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
    public TickAnalysisWindow(String name, int barLength) {
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
    Indicator<Num> kama0 = null;
    Indicator<Num> kama1 = null;


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

        lr = new SimpleLinearRegressionIndicator(closePrice, 60, SimpleLinearRegressionIndicator.SimpleLinearRegressionType.Y);

        kama0 = new KAMAIndicator(lrClosePrice, 10, 2, 30);
        kama1 = new KAMAIndicator(lrClosePrice, 10, 3, 45);

        Indicator<Num> meanDeviation = new MeanDeviationIndicator(lrClosePrice, barLength);

        indicatorMap.put("lr",  lr);
        indicatorMap.put("kama0",  kama0);
        indicatorMap.put("kama1", kama1);
       // indicatorMap.put("arup", new AroonUpIndicator(closePrice, barLength));
      //  indicatorMap.put("ardown", new AroonDownIndicator(closePrice, barLength));

     //   indicatorMap.put("adx", new ADXIndicator(lrSeries, 24, 60));
        indicatorMap.put("pdi", new PlusDIIndicator(lrSeries,  barLength));
        indicatorMap.put("mdi", new MinusDIIndicator(lrSeries,  barLength));


        lwma = new LWMAIndicator(lrClosePrice, 10  * barLength);
        rsi = new RSIIndicator(lrClosePrice, 5 * barLength);

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



        indicatorMap.put("low5", new LowestValueIndicator(lr, 5));
        indicatorMap.put("low10", new LowestValueIndicator(lr, 10));
        indicatorMap.put("low15", new LowestValueIndicator(lr, 15));
        indicatorMap.put("low20", new LowestValueIndicator(lr, 20));
        indicatorMap.put("low30", new LowestValueIndicator(lr, 30));

        indicatorMap.put("low60", new LowestValueIndicator(lr, 60));
        indicatorMap.put("low120", new LowestValueIndicator(lr, 120));
        indicatorMap.put("low180", new LowestValueIndicator(lr, 180));


        indicatorMap.put("high10", new HighestValueIndicator(lr, 10));
        indicatorMap.put("high15", new HighestValueIndicator(lr, 15));
        indicatorMap.put("high20", new HighestValueIndicator(lr, 20));
        indicatorMap.put("high30", new HighestValueIndicator(lr, 30));
        indicatorMap.put("high60", new HighestValueIndicator(lr, 60));
        indicatorMap.put("high120", new HighestValueIndicator(lr, 120));
        indicatorMap.put("high180", new HighestValueIndicator(lr, 180));

        // indicatorMap.put("roc15v4", new RSIIndicator(lrClosePrice, 15));




        Indicator<Num> kamaDiff  = TransformIndicator.abs(new DifferenceIndicator(kama0, kama1));

        Indicator<Num> lr1KamaDiff  = TransformIndicator.abs(new DifferenceIndicator(kama0, lrClosePrice));

        Indicator<Num> lr2KamaDiff  = TransformIndicator.abs(new DifferenceIndicator(kama1, lrClosePrice));

        tradeInProfit = new TradeInProfileIndicator(closePrice);



        Indicator<Num> chop = new ChopIndicator(lrSeries, 5  * barLength, 1);
        indicatorMap.put("chop", chop);


        StandardDeviationIndicator std = new StandardDeviationIndicator(closePrice, barLength);

        indicatorMap.put("std", std);
        indicatorMap.put("meand", meanDeviation);

        indicatorMap.put("rsi", rsi);

//
//        indicatorMap.put("srsi", new StochasticRSIIndicator(lrClosePrice, 60));
//
//        indicatorMap.put("srsi", new StochasticRSIIndicator(lrClosePrice, 60));
//
//        StochasticOscillatorKIndicator stok = new StochasticOscillatorKIndicator(lrSeries, 60);
//        indicatorMap.put("stok", stok);
//
//        indicatorMap.put("stod", new StochasticOscillatorDIndicator(stok));



        // indicatorMap.put("variance", variance1);

        // NIFTY: 0.5
        //
        double callEnterDiff = .1;
        double putEnterDiff = .1;
        //double diff = 3; // Bank

        // CHOP : Low means active market, high means consolidation or not moving..

//        Rule callEnterRule =   new OverIndicatorRule(kama0, kama1)
//                .and(new UnderIndicatorRule(kama0, lr))
//                .and(new UnderIndicatorRule(kama1, lr))
//              //   .and(new OverIndicatorRule(getIndicator("lr"), getIndicator("low10")))
//           //     .and(new OverIndicatorRule(getIndicator("low5"), getIndicator("low10")))
//          //       .and(new OverIndicatorRule(getIndicator("low10"), getIndicator("low15")))
//           //     .and(new OverIndicatorRule(getIndicator("pdi"), getIndicator("mdi")))
//            //     .and(new OverIndicatorRule(getIndicator("pdi"), DecimalNum.valueOf(54)))
//                    .and(new OverIndicatorRule(lr, lwma));
//
//

        Rule callEnterRule =   new UnderIndicatorRule(kama0, lr)
              //     .and(new OverIndicatorRule(getIndicator("pdi"), getIndicator("mdi")))
             //         .and(new OverIndicatorRule(getIndicator("pdi"), DecimalNum.valueOf(51)))
                .and(new OverIndicatorRule(lr, lwma));



             //  .and(new OverIndicatorRule(kamaDiff, DecimalNum.valueOf(callEnterDiff)))
              //   .and(new OverIndicatorRule(lr1KamaDiff, DecimalNum.valueOf(callEnterDiff)))
              //  .and(new OverIndicatorRule(lr2KamaDiff, DecimalNum.valueOf(callEnterDiff)))

               // .and(new UnderIndicatorRule(rsi, DecimalNum.valueOf(60)))
              //  .and(new OverIndicatorRule(rsi, DecimalNum.valueOf(45)))
               // .and(new OverIndicatorRule(lr, bollingerLower))
               // .and(new UnderIndicatorRule(lr, bollingerMiddle))
           //     .and(new OverIndicatorRule(getIndicator("pdi"), getIndicator("mdi")))
               // .and(new OverIndicatorRule(getIndicator("pdi"), DecimalNum.valueOf(50)))
             //   .and(new UnderIndicatorRule(getIndicator("mdi"), DecimalNum.valueOf(48)))
           //     .and(new OverIndicatorRule(getIndicator("lr"), getIndicator("low10")))
           //     .and(new OverIndicatorRule(getIndicator("low10"), getIndicator("low20")))
            //    .and(new OverIndicatorRule(getIndicator("lr"), getIndicator("low30")))

            //    .and(new OverIndicatorRule(new DifferenceIndicator(getIndicator("lr"), getIndicator("low10")), DecimalNum.valueOf(2)))
             //   .and(new OverIndicatorRule(new DifferenceIndicator(getIndicator("lr"), getIndicator("low15")), DecimalNum.valueOf(2)))
             //   .and(new OverIndicatorRule(new DifferenceIndicator(getIndicator("lr"), getIndicator("low20")), DecimalNum.valueOf(2)));

               // .and(new UnderIndicatorRule(getIndicator("kama2"), getIndicator("lr")));

        //    .and(new OverIndicatorRule(getIndicator("low60"), getIndicator("low120")))
          //      .and(new OverIndicatorRule(getIndicator("low120"), getIndicator("low180")));

        //    .and(new OverIndicatorRule(std, DecimalNum.valueOf(1)))
        //        .and(new UnderIndicatorRule(chop, DecimalNum.valueOf(0.6)));


               // .and(new OverIndicatorRule(highest60, highest180));
               // .and(new OverIndicatorRule(variance1, DecimalNum.valueOf(3)));
              //  .and(new UnderIndicatorRule(kamaDiff, DecimalNum.valueOf(2)));

      //  Rule callExitRule =    new OverIndicatorRule(kama0, lr).and(new OverIndicatorRule(kama1, lr));
                //          .and(new OverIndicatorRule(getIndicator("mdi"), getIndicator("pdi")))
                //  .and(new OverIndicatorRule(new DifferenceIndicator(getIndicator("high10"), getIndicator("lr")), DecimalNum.valueOf(2)))
                //   .and(new OverIndicatorRule(new DifferenceIndicator(getIndicator("high20"), getIndicator("lr")), DecimalNum.valueOf(2)))

        //,  new OverIndicatorRule(tradeInProfit, DecimalNum.valueOf(0)));
//
//        Rule callExitRule =   new OrRule(new OverIndicatorRule(kama0, lr).and(new OverIndicatorRule(kama1, lr))
//                 .and(new OverIndicatorRule(getIndicator("mdi"), getIndicator("pdi")))
//        ,  new OverIndicatorRule(tradeInProfit, DecimalNum.valueOf(0)));


//        Rule callExitRule =   new AndRule(new OverIndicatorRule(kama0, lr)
//                ,(new OverIndicatorRule(kama1, lr)));
//
        Rule callExitRule =    new OverIndicatorRule(kama0, lr);
               // .and(new OverIndicatorRule(getIndicator("mdi"), getIndicator("pdi")));



        callStrategy = new BaseStrategy(callEnterRule, callExitRule);


//        Rule putEnterRule =   new UnderIndicatorRule(kama0, kama1)
//                .and(new OverIndicatorRule(kama0, lr))
//                .and(new OverIndicatorRule(kama1, lr))
//                .and(new OverIndicatorRule(kamaDiff, DecimalNum.valueOf(putEnterDiff)))
//                .and(new OverIndicatorRule(lr1KamaDiff, DecimalNum.valueOf(putEnterDiff)))
//                .and(new OverIndicatorRule(lr2KamaDiff, DecimalNum.valueOf(putEnterDiff)))
//                .and(new UnderIndicatorRule(lr, lwma))
//                .and(new OverIndicatorRule(rsi, DecimalNum.valueOf(30)))
//                .and(new UnderIndicatorRule(rsi, DecimalNum.valueOf(60)))
//                .and(new UnderIndicatorRule(lr, bollingerUp))
//                .and(new  OverIndicatorRule(lr, bollingerMiddle));
//              //  .and(new OverIndicatorRule(variance1, DecimalNum.valueOf(3)));

        // new OverIndicatorRule(kama0, kama1)


        Rule putEnterRule =    new OverIndicatorRule(kama0, lr)
                .and(new UnderIndicatorRule(lr, lwma));
              //  .and(new UnderIndicatorRule(getIndicator("pdi"), getIndicator("mdi")));

        Rule putExitRule =    new UnderIndicatorRule(kama0, lr);
             //   .and(new UnderIndicatorRule(getIndicator("mdi"), getIndicator("pdi")));

        putStrategy = new BaseStrategy(putEnterRule, putExitRule);
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



        // ---
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

//
//        try {
//            Num diff = lr.getValue(index).minus(lr.getValue( index - 15));
//            Num diff2 = lr.getValue(index - 1).minus(lr.getValue( index - 16));
//            Num diff3 = lr.getValue(index - 2).minus(lr.getValue( index - 17));
//            if (diff.isGreaterThanOrEqual(DecimalNum.valueOf(4)) && diff2.isGreaterThanOrEqual(DecimalNum.valueOf(4)) && diff3.isGreaterThanOrEqual(DecimalNum.valueOf(4)) ) {
//                taResult.fields.put("roc15v4",   diff.doubleValue());
//            }
//        }catch (Exception e) {
//
//        }

        try {
            int INTERVAL = 12;
            double DIFF = 4;
            double ROC_DIFF = 0.1;

            Indicator<Num> roc = getIndicator("roc");
            Num roc0 = roc.getValue(index - INTERVAL - 4);

            Num diff = lr.getValue(index).minus(lr.getValue( index - INTERVAL));
            Num diff2 = lr.getValue(index).minus(lr.getValue( index - INTERVAL - 2));
            Num diff3 = lr.getValue(index).minus(lr.getValue( index - INTERVAL - 3));

            if (roc0.isLessThan(DecimalNum.valueOf(ROC_DIFF)) && lr.getValue(index).isGreaterThan(lwma.getValue(index)) && diff.isGreaterThanOrEqual(DecimalNum.valueOf(DIFF)) && diff2.isGreaterThanOrEqual(DecimalNum.valueOf(DIFF)) && diff3.isGreaterThanOrEqual(DecimalNum.valueOf(DIFF)) ) {
                taResult.fields.put("roc12v4",   diff.doubleValue());
            }
        }catch (Exception e) {

        }

//        if (getIndicator("roc15v4").getValue(index).isGreaterThanOrEqual(DecimalNum.valueOf(4))) {
//            taResult.fields.put("roc15v4",   getIndicator("roc15v4").getValue(index).doubleValue());
//        }

       // System.out.println(taResult);

        collector.collect(taResult);
    }

}