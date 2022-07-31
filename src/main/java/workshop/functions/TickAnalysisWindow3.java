package workshop.functions;

import workshop.models.Tick;
import workshop.models.TickTA;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBarSeriesBuilder;
import org.ta4j.core.Indicator;
import org.ta4j.core.Rule;
import org.ta4j.core.indicators.*;
import org.ta4j.core.indicators.helpers.*;
import org.ta4j.core.indicators.statistics.SimpleLinearRegressionIndicator;
import org.ta4j.core.indicators.volume.VWAPIndicator;
import org.ta4j.core.num.DecimalNum;
import org.ta4j.core.num.Num;
import org.ta4j.core.rules.OverIndicatorRule;
import org.ta4j.core.rules.UnderIndicatorRule;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class TickAnalysisWindow3 extends KeyedProcessFunction<String, Tick, TickTA> {

    static     class KAMAERIndicator extends RecursiveCachedIndicator<Num> {
        private final Indicator<Num> price;
        private final int barCountEffectiveRatio;

        public KAMAERIndicator(Indicator<Num> price, int barCountEffectiveRatio) {
            super(price);
            this.price = price;
            this.barCountEffectiveRatio = barCountEffectiveRatio;

        }

        public KAMAERIndicator(Indicator<Num> price) {
            this(price, 10);
        }

        protected Num calculate(int index) {
            Num currentPrice = (Num)this.price.getValue(index);
            if (index < this.barCountEffectiveRatio) {
                return currentPrice;
            } else {
                int startChangeIndex = Math.max(0, index - this.barCountEffectiveRatio);
                Num change = currentPrice.minus((Num)this.price.getValue(startChangeIndex)).abs();
                Num volatility = this.numOf(0);

                for(int i = startChangeIndex; i < index; ++i) {
                    volatility = volatility.plus(((Num)this.price.getValue(i + 1)).minus((Num)this.price.getValue(i)).abs());
                }

                Num er = change.dividedBy(volatility);

                // adjust er

                if (volatility.abs().isLessThan(DecimalNum.valueOf(3))) {
                    return DecimalNum.valueOf(0);
                }

                return er;
            }
        }
    }

    ValueState<BarSeries> seriesState;
    ClosePriceIndicator closePrice = null;

    ClosePriceIndicator closePrice2= null;

    BarSeries series2;
    BarSeries series;
    ValueState<Integer> signalState;
    private ValueStateDescriptor<Integer> signalStateDescriptor = new ValueStateDescriptor("Signal",Integer.class);

    private ValueStateDescriptor<Tick> lastTickStateDescriptor = new ValueStateDescriptor("LastTick",Tick.class);
    ValueState<Tick> lastTickState;

    ValueStateDescriptor<BarSeries> barSeriesState;
    String name;

    int barLength = 30;
    Indicator<Num> roc = null;
    Indicator<Num> rocAbs = null;
    public TickAnalysisWindow3(String name, int barLength) {
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

    Indicator<Num> hv = null;
    Indicator<Num> lv = null;

    //Indicator<Num> zl = null;

    Indicator<Num> vwap = null;


    ArrayList<Integer> lhWindows = new ArrayList<Integer>(
            Arrays.asList(5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 300, 480, 600, 900, 1200, 1500, 1800));

    ArrayList<Integer> vWapWindows = new ArrayList<Integer>(
            Arrays.asList(1,2,3,5,10,15,30,45,60));

    // for zero lag, kama indicator
    ArrayList<Integer> zlWindows = new ArrayList<Integer>(
            Arrays.asList(5, 13, 21, 35));



    void build() {
        series = new BaseBarSeriesBuilder().withMaxBarCount(1000000).withName("my_2017_series").build();
        series2 = new BaseBarSeriesBuilder().withMaxBarCount(1000000).withName("my_2012_series").build();

        closePrice = new ClosePriceIndicator(series);
        closePrice2 =  new ClosePriceIndicator(series2);

        lr = new SimpleLinearRegressionIndicator(closePrice, 30, SimpleLinearRegressionIndicator.SimpleLinearRegressionType.Y);


        indicatorMap.put("lr",  lr);
        // indicatorMap.put("lri",  new SimpleLinearRegressionIndicator(closePrice, barLength, SimpleLinearRegressionIndicator.SimpleLinearRegressionType.INTERCEPT));
        indicatorMap.put("lrs",  new SimpleLinearRegressionIndicator(closePrice, barLength, SimpleLinearRegressionIndicator.SimpleLinearRegressionType.SLOPE));

        vwap = new VWAPIndicator(series, barLength);
        indicatorMap.put("vwap",  vwap);

        roc = new ROCIndicator(closePrice2, barLength);
        rocAbs = TransformIndicator.abs(roc);

        indicatorMap.put("roc",  roc);
//
//        zl = new ZLEMAIndicator(closePrice2,  13); // it was barLength, reducing to five for now..
//        indicatorMap.put("zl",  zl);

        indicatorMap.put("arup",  new AroonUpIndicator(closePrice2, barLength));
        indicatorMap.put("ardown",  new AroonDownIndicator(closePrice2, barLength));
        //indicatorMap.put("aroc",  new AroonOscillatorIndicator(series, barLength));
        //indicatorMap.put("wr",  new WilliamsRIndicator(series, barLength));

        //indicatorMap.put("cci",  new CCIIndicator(series, barLength));
        //indicatorMap.put("rsi",  new RSIIndicator(lr, barLength));

        //indicatorMap.put("tema",  new TripleEMAIndicator(closePrice, barLength));
        //indicatorMap.put("dema",  new DoubleEMAIndicator(closePrice, barLength));
        ema = new EMAIndicator(closePrice, barLength);
        indicatorMap.put("ema",  ema);
         lwma = new LWMAIndicator(closePrice, barLength);
        indicatorMap.put("lwma", lwma);

        hv = new HighestValueIndicator(closePrice, barLength);
        indicatorMap.put("hv", hv );
        lv = new LowestValueIndicator(closePrice, barLength);
        indicatorMap.put("lv", lv);

        for (int timeSlide: lhWindows) {
            String name = "hv" + timeSlide;
            Indicator<Num> indicator = new HighestValueIndicator(closePrice, timeSlide);
            indicatorMap.put(name, indicator);
        }

        for (int timeSlide: lhWindows) {
            String name = "lv" + timeSlide;
            Indicator<Num> indicator = new LowestValueIndicator(closePrice, timeSlide);
            indicatorMap.put(name, indicator);
        }

        for (int timeSlide: vWapWindows) {
            String name = "vwap" + timeSlide;
            Indicator<Num> indicator = new VWAPIndicator(series, timeSlide * 60);
            indicatorMap.put(name, indicator);
        }

        //Ration: 10:2:30
        int i = 0;
        ArrayList<Tuple3<Integer, Integer, Integer>> kamaWindows = new ArrayList<Tuple3<Integer, Integer, Integer>>(
                Arrays.asList(
                        new Tuple3<Integer, Integer, Integer>(10, 2, 30),
                        new Tuple3<Integer, Integer, Integer>(20, 3, 60),
                        new Tuple3<Integer, Integer, Integer>(40, 6, 120)
        ));


        for (Tuple3<Integer, Integer, Integer> timeSlide: kamaWindows) {

            Indicator<Num> kama = new KAMAIndicator(closePrice2, timeSlide.f0, timeSlide.f1, timeSlide.f2);
            String name2 = "kama" + i;
            indicatorMap.put(name2, kama);

            Indicator<Num> er = new KAMAERIndicator(closePrice2, timeSlide.f0);
            String name3 = "er" + i;
            indicatorMap.put(name3, er);
            i++;
        }

        lrHighDiffIndicator = new DifferenceIndicator(hv, closePrice);
         lrCloseDiffIndicator = new DifferenceIndicator(closePrice,  lv);

         //  if (Math.abs(rocValue) <= 0.00099) {

        DecimalNum rocConstant = DecimalNum.valueOf(0.005); //0.00099  // 0.0019 //0.0030 // 0.0099 //0.0099
//
//          shortRule = new UnderIndicatorRule(zl, hv)
//                        .and(new OverIndicatorRule(lrHighDiffIndicator, DecimalNum.valueOf(1.5)))
//                  .and (new UnderIndicatorRule(zl, lwma))
//                  .and (new UnderIndicatorRule(zl, ema))
//                  .and (new UnderIndicatorRule(rocAbs, rocConstant));
//
//          longRule = new OverIndicatorRule(lrCloseDiffIndicator, DecimalNum.valueOf(1.5))
//                        .and(new OverIndicatorRule(zl, lv))
//                        .and (new OverIndicatorRule(zl, lwma))
//                        .and (new OverIndicatorRule(zl, ema))
//                        .and (new UnderIndicatorRule(rocAbs, rocConstant));


        Indicator<Num> kama0 = indicatorMap.get("kama0");
        Indicator<Num> kama1 = indicatorMap.get("kama1");
        Indicator<Num> kama2 = indicatorMap.get("kama2");



        longRule =  new OverIndicatorRule(closePrice, lv)
                .and(new OverIndicatorRule(kama0, kama1))
                .and(new OverIndicatorRule(kama1, kama2))
                .and(new OverIndicatorRule(kama0, kama2))
                .and(new UnderIndicatorRule(kama0, closePrice))
                .and(new UnderIndicatorRule(kama1, closePrice))
                .and(new UnderIndicatorRule(kama2, closePrice))
            //    .and (new UnderIndicatorRule(getIndicator("arup"),  DecimalNum.valueOf(5)))
            //    .and (new OverIndicatorRule(getIndicator("ardown"),  DecimalNum.valueOf(95)))
                //.and (new UnderIndicatorRule(zl, ema))
                .and (new UnderIndicatorRule(rocAbs, rocConstant));

        exitLongRule =  new UnderIndicatorRule(kama0, kama1)
                .and(new UnderIndicatorRule(kama1, kama2))
                .and(new UnderIndicatorRule(kama0, kama2))
                .and(new OverIndicatorRule(kama0, closePrice))
                .and(new OverIndicatorRule(kama1, closePrice))
                .and(new OverIndicatorRule(kama2, closePrice));

        shortRule =  new UnderIndicatorRule(closePrice, hv)
                .and(new UnderIndicatorRule(kama0, kama1))
                .and(new UnderIndicatorRule(kama1, kama2))
                .and(new UnderIndicatorRule(kama0, kama2))
                .and(new OverIndicatorRule(kama0, closePrice))
                .and(new OverIndicatorRule(kama1, closePrice))
                .and(new OverIndicatorRule(kama2, closePrice))
              //  .and (new OverIndicatorRule(getIndicator("ardown"),  DecimalNum.valueOf(95)))
              // .and (new UnderIndicatorRule(getIndicator("arup"),  DecimalNum.valueOf(5)))
                //.and (new UnderIndicatorRule(zl, ema))
                .and (new UnderIndicatorRule(rocAbs, rocConstant));

        exitShortRule =  new OverIndicatorRule(kama0, kama1)
                .and(new OverIndicatorRule(kama1, kama2))
                .and(new OverIndicatorRule(kama0, kama2))
                .and(new UnderIndicatorRule(kama0, closePrice))
                .and(new UnderIndicatorRule(kama1, closePrice))
                .and(new UnderIndicatorRule(kama2, closePrice));

    }


    public Indicator<Num>  getIndicator(String name) {
        return indicatorMap.get(name);
    }

    Indicator lwma = null;

    Indicator ema = null;

            Indicator<Num> lrHighDiffIndicator = null;
    Indicator<Num> lrCloseDiffIndicator = null;
    Rule shortRule = null;
    Rule exitShortRule = null;

    Rule longRule = null;
    Rule exitLongRule = null;

    Num getValue(String name, int index ) {
        Indicator<Num> indicator = indicatorMap.get(name);
        return indicator.getValue(index);
    }

    double getDoubleValue(String name, int index ) {
        Indicator<Num> indicator = indicatorMap.get(name);
        return indicator.getValue(index).doubleValue();
    }

    @Override
    public void processElement(
            Tick tick ,
            Context context,
            Collector<TickTA> collector) throws Exception {

            signalState = getRuntimeContext().getState(signalStateDescriptor);

            if (signalState.value() == null) {
                signalState.update(0); // 0 - neutral, no signal or nothing
            }

        lastTickState = getRuntimeContext().getState(lastTickStateDescriptor);

        Double volume = 0.0;
        Double amount = 0.0;

         if (lastTickState.value() == null) {
             lastTickState.update(tick);
             volume = tick.Vol;
             amount = tick.DTO;
         } else {
             Tick lastTick = lastTickState.value();
             if (tick.Vol > 0) {
                volume = tick.Vol - lastTick.Vol;
             }

             if (tick.DTO > 0) {
                 amount = tick.DTO - lastTick.DTO;
             }
         }

        ZonedDateTime d = ZonedDateTime.ofInstant(tick.ts.toInstant(), ZoneId.of("UTC"));
        try {
            series.addBar(d, tick.LTP, tick.LTP, tick.LTP, tick.LTP, volume, amount);
        }
        catch (IllegalArgumentException iae) {
         //   System.out.println("ERROR " + iae.getMessage());
        }
        catch(Exception e) {
            throw e;
        }
        int index = series.getEndIndex();

        try {
            Num value2 = lr.getValue(index);

            series2.addBar(d, value2.doubleValue(), value2.doubleValue(), value2.doubleValue(), value2.doubleValue(), volume, amount);
            System.out.println("LR Value " + value2.doubleValue());
        }
        catch (IllegalArgumentException iae) {
               System.out.println("ERROR R2 " + iae.getMessage());
        }
        catch(Exception e) {
            System.out.println("ERror " + e);
            throw e;
        }

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

        taResult.fields.put("VOL", volume);
        taResult.fields.put("DTO", amount);
        // double rocValue =  roc.getValue(index).doubleValue();

        if (shortRule.isSatisfied(index)) {
            taResult.fields.put("short", 1.0);
        }

        if (longRule.isSatisfied(index)) {
            taResult.fields.put("long", 1.0);
        }

        if (exitLongRule.isSatisfied(index)) {
            taResult.fields.put("exitLong", 1.0);
        }

        if (exitShortRule.isSatisfied(index)) {
            taResult.fields.put("exitShort", 1.0);
        }

        // 3 minute reversals



        lastTickState.update(tick);



        collector.collect(taResult);
    }

}