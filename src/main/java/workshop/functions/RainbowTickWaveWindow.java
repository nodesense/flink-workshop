package workshop.functions;

import workshop.models.DataValueMap;
import workshop.models.Tick;
import one.util.streamex.StreamEx;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBarSeriesBuilder;
import org.ta4j.core.Indicator;
import org.ta4j.core.indicators.*;
import org.ta4j.core.indicators.helpers.ClosePriceIndicator;
import org.ta4j.core.indicators.statistics.SimpleLinearRegressionIndicator;
import org.ta4j.core.num.Num;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Optional;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class RainbowTickWaveWindow extends KeyedProcessFunction<String, Tick, DataValueMap> {
    ValueState<BarSeries> seriesState;
    ClosePriceIndicator closePrice = null;

    BarSeries series;
    ValueState<Integer> signalState;
    private ValueStateDescriptor<Integer> signalStateDescriptor = new ValueStateDescriptor("Signal",Integer.class);
    ValueStateDescriptor<BarSeries> barSeriesState;
    String indicatorType;
    int factor = 2;

    int support = 0;

    Indicator<Num> roc = null;
    public RainbowTickWaveWindow(String indicatorType, int factor) {
        this.indicatorType = indicatorType;
        this.factor = factor;
        this.support = this.factor * 24 * 5;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        barSeriesState   = new ValueStateDescriptor<>("barSeriesState", BarSeries.class);
        build();
    }

    private static final long serialVersionUID = 1L;
    ArrayList<Indicator> indicators = new ArrayList<>();
    Indicator<Num> williamR = null;
    Indicator<Num> cci = null;

    Indicator<Num> rsi = null;

    Indicator<Num> sr = null;

    Indicator<Num> aroonUp = null;
    Indicator<Num> aroonDown = null;
    Indicator<Num> aroonOC = null;
    SimpleLinearRegressionIndicator linearR = null;




    void build() {
        series = new BaseBarSeriesBuilder().withMaxBarCount(1000000).withName("my_2017_series").build();

        closePrice = new ClosePriceIndicator(series);

        for (int i = 1; i <= 24; i++) {
            //Indicator<Num> indicator = new DoubleEMAIndicator(closePrice, i * factor);
            //Indicator<Num> indicator = new WMAIndicator(closePrice, i * factor);
            // Indicator<Num> indicator = new TripleEMAIndicator(closePrice, i * factor);
            Indicator<Num> indicator = new ZLEMAIndicator(closePrice, i * factor);
            indicators.add(indicator);
        }

        williamR = new WilliamsRIndicator(series, 30);

        cci = new CCIIndicator(series, 30);
        rsi = new RSIIndicator(closePrice, 30);

        sr = new TripleEMAIndicator(closePrice, support);
        roc = new ROCIndicator(closePrice, 30);
        aroonUp = new AroonUpIndicator(closePrice, 30);
        aroonDown = new AroonDownIndicator(closePrice, 30);
        aroonOC = new AroonOscillatorIndicator(series, 30);

        linearR = new SimpleLinearRegressionIndicator(closePrice, 30, SimpleLinearRegressionIndicator.SimpleLinearRegressionType.Y);

    }

    double getValueFromIndicator(int i, int index) {
        Indicator<Num> indicator = indicators.get(i);
        return indicator.getValue(index).doubleValue();
    }

    @Override
    public void processElement(
            Tick tick ,
            Context context,
            Collector<DataValueMap> collector) throws Exception {

            signalState = getRuntimeContext().getState(signalStateDescriptor);

            if (signalState.value() == null) {
                signalState.update(0); // 0 - neutral, no signal or nothing
            }

//         if (seriesState.value() == null ) {
//            series = new BaseBarSeriesBuilder().withMaxBarCount(1000000).withName("my_2017_series").build();
//            seriesState.update(series);
//        } else {
//            series = seriesState.value(); //??
//        }
//
//        if (closePrice == null) {
//            build();
//        }


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
        int index = series.getEndIndex();

        if (index < 24) return;

       // if (index < support) return;

   //     System.out.println("Index " + index +   " Removed count " +  series.getRemovedBarsCount() +  " Bar BCount " +  series.getBarCount());
        DataValueMap resultValue = new DataValueMap();

        resultValue.asset = tick.asset;
        resultValue.ts = tick.ts;

        resultValue.wave1 = getValueFromIndicator(0, index);
        resultValue.wave2 = getValueFromIndicator(1, index);
        resultValue.wave3 = getValueFromIndicator(2, index);
        resultValue.wave4 = getValueFromIndicator(3, index);
        resultValue.wave5 = getValueFromIndicator(4, index);
        resultValue.wave6 = getValueFromIndicator(5, index);
        resultValue.wave7 = getValueFromIndicator(6, index);
        resultValue.wave8 = getValueFromIndicator(7, index);
        resultValue.wave9 = getValueFromIndicator(8, index);
        resultValue.wave10 = getValueFromIndicator(9, index);
        resultValue.wave11 = getValueFromIndicator(10, index);
        resultValue.wave12 = getValueFromIndicator(11, index);
        resultValue.wave13 = getValueFromIndicator(12, index);
        resultValue.wave14 = getValueFromIndicator(13, index);
        resultValue.wave15 = getValueFromIndicator(14, index);
        resultValue.wave16 = getValueFromIndicator(15, index);
        resultValue.wave17 = getValueFromIndicator(16, index);
        resultValue.wave18 = getValueFromIndicator(17, index);
        resultValue.wave19 = getValueFromIndicator(18, index);
        resultValue.wave20 = getValueFromIndicator(19, index);
        resultValue.wave21 = getValueFromIndicator(20, index);
        resultValue.wave22 = getValueFromIndicator(21, index);
        resultValue.wave23 = getValueFromIndicator(22, index);
        resultValue.wave24 = getValueFromIndicator(23, index);

        resultValue.williamr = williamR.getValue(index).doubleValue();
        resultValue.cci = williamR.getValue(index).doubleValue();
        resultValue.rsi = rsi.getValue(index).doubleValue();
        resultValue.roc = roc.getValue(index).doubleValue();

        roc = new ROCIndicator(closePrice, 30);


        aroonUp = new AroonUpIndicator(closePrice, 30);
        aroonDown = new AroonDownIndicator(closePrice, 30);
        aroonOC = new AroonOscillatorIndicator(series, 30);
        linearR = new SimpleLinearRegressionIndicator(closePrice, 30);

        resultValue.ltp = tick.LTP;
        resultValue.arup = aroonUp.getValue(index).doubleValue();
        resultValue.ardown = aroonDown.getValue(index).doubleValue();
        resultValue.aroc = aroonOC.getValue(index).doubleValue();
        resultValue.lreg = linearR.getValue(index).doubleValue();


        ArrayList<Double> values = new ArrayList<>();

        for (int i = 0; i < 24; i++) {
            int index2 = series.getEndIndex();
            // if (index2 <  24 ) continue;

           Indicator<Num> indicator = indicators.get(i);

            try {
                Num n = indicator.getValue(index2);
                //resultValue.fields.put("wave" + (i + 1), n.doubleValue());
            //    System.out.println("REsult " +  index2 + " : " + n.doubleValue());
                values.add(n.doubleValue());
            }catch(Exception e) {
          //      System.out.println("Index " + index2 + " " + e.getMessage());
            }
        }


        // values.add(sr.getValue(series.getEndIndex()).doubleValue());
        boolean buy = StreamEx.of(values).pairMap((a, b) -> a > b).allMatch(e -> e); // true

        // values.remove(values.size() - 1);

        boolean sell = StreamEx.of(values).pairMap((a, b) -> a < b).allMatch(e -> e); // false

        Optional<Double> buyDiffSum = StreamEx.of(values).pairMap((a, b) -> a - b).reduce( (acc, value) -> acc + value);

        Optional<Double> sellDiffSum = StreamEx.of(values).pairMap((a, b) -> b - a).reduce( (acc, value) -> acc + value);

        resultValue.adiff =  (double) (double) buyDiffSum.get();
        resultValue.sdiff =  (double) (double) sellDiffSum.get();



//        int existingSignal = signalState.value();
//
//        if ( (existingSignal == 0) && buy) {
//            signalState.update(1);
//        } else if ( (existingSignal == 0) && sell) {
//            signalState.update(-1);
//        }
//        else {
//            signalState.update(0);
//        }

        if (buy) {
            if (signalState.value() == 1) { // we already send 1
                signalState.update(2);
            } else if (signalState.value() < 1) {
                signalState.update(1);
            }
        }

        if (sell) {
            if (signalState.value() == -1) { // we already send -1
                signalState.update(-2);
            } else if (signalState.value() > -1) {
                signalState.update(-1);
            }
        }

        if (!buy && !sell) {
            signalState.update(0);
        }

        resultValue.signal =  (double) signalState.value();


//        resultValue.fields.put("signal", (double) signalState.value());
//        resultValue.fields.put("addDiff", (double) buyDiffSum.get());
//        resultValue.fields.put("subDiff", (double) sellDiffSum.get());

//
//        resultValue.fields.put("wave1", 100.2);
//        resultValue.fields.put("wave2", 100.4);
//        resultValue.fields.put("wave3", 100.5);

        collector.collect(resultValue);

//        Num result = indicator.getValue(index);
//
//      if (result != null) {
//          // FIXME: Make a clone of value
//          dataValue.value = result.doubleValue();
//          DataValueMap resultValue = new DataValueMap();
//          collector.collect(resultValue);
//      }

    }

}