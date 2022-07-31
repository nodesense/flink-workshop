package workshop.functions;

import workshop.models.DataValue;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBarSeriesBuilder;
import org.ta4j.core.Indicator;
import org.ta4j.core.indicators.SMAIndicator;
import org.ta4j.core.indicators.helpers.ClosePriceIndicator;
import org.ta4j.core.num.Num;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class TAWindow  extends KeyedProcessFunction<String, DataValue, DataValue> {
    ValueState<BarSeries> seriesState;
    ClosePriceIndicator closePrice;
    Indicator<Num>  indicator;
    BarSeries series;
    private ValueStateDescriptor<DataValue> maxDataValueState = new ValueStateDescriptor("TAWindow",DataValue.class);
    ValueStateDescriptor<BarSeries> barSeriesState;
    String indicatorType;
    int length = -1;
    public TAWindow(String indicatorType, int length) {
        this.indicatorType = indicatorType;
        this.length = length;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        barSeriesState   = new ValueStateDescriptor<>("barSeriesState", BarSeries.class);
    }

    private static final long serialVersionUID = 1L;


    @Override
    public void processElement(
            DataValue dataValue ,
            Context context,
            Collector<DataValue> collector) throws Exception {

        seriesState = getRuntimeContext().getState(barSeriesState);

        if (seriesState.value() == null ) {
            series = new BaseBarSeriesBuilder().withName("my_2017_series").build();
            seriesState.update(series);
            closePrice = new ClosePriceIndicator(series);
            indicator = new SMAIndicator(closePrice, length);
        } else {
            series = seriesState.value(); //??
        }


        ZonedDateTime d = ZonedDateTime.ofInstant(dataValue.ts.toInstant(), ZoneId.of("UTC"));
        series.addBar(d, dataValue.value, dataValue.value, dataValue.value, dataValue.value);

        int index = series.getEndIndex();

        Num result = indicator.getValue(index);

      if (result != null) {
          // FIXME: Make a clone of value
          dataValue.value = result.doubleValue();
          collector.collect(dataValue);
      }

    }

}