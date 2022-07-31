package workshop.functions;

import workshop.models.DataValue;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class MaxWindowFunction extends  ProcessWindowFunction<DataValue,DataValue, String, TimeWindow> {

    private ValueStateDescriptor<DataValue> maxDataValueState = new ValueStateDescriptor("MinSumDataValueState",Double.class);

    public MaxWindowFunction() {

    }

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String key, Context context, Iterable<DataValue> iterable, Collector<DataValue> collector) throws Exception {
        DataValue maxDataValue = null;
        System.out.println("Iterator Begin " + new Date());
        ValueState<DataValue> maxVal = context.globalState().getState(maxDataValueState);

        String output_tag = null;
        System.out.println("Key::" + key);

        DataValue max = maxVal.value();
//
//
        System.out.println("Starting sum Value " + max);
        System.out.println(key + "->  start sum " + max);
//		System.out.println(key + "-> start samples" + samples);


        for(DataValue dataValue : iterable){
            System.out.println("PROCESSOR " + dataValue + " THREAD " + Thread.currentThread().getId());

            if(max == null){
                max = dataValue;
            }

            if (max.value < dataValue.value){
                max = dataValue;
            }


// 			System.out.println("maxDataValue " +  maxDataValue);
        }

        System.out.println(key + "->  end sum " + max);
//		System.out.println(key + "-> end samples" + samples);
//
        System.out.println("Iterator End" + new Date());

        maxVal.update(max);


        collector.collect(max);
    }
}