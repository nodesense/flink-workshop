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
public class MaxWindowFunction2 extends  ProcessWindowFunction<DataValue,DataValue, String, TimeWindow> {

    private ValueStateDescriptor<Double> maxDataValueState = new ValueStateDescriptor("MinSumDataValueState",Double.class);

    public MaxWindowFunction2() {

    }

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String key, Context context, Iterable<DataValue> iterable, Collector<DataValue> collector) throws Exception {
        DataValue maxDataValue = null;
        System.out.println("Iterator Begin " + new Date());
        ValueState<Double> maxVal = context.globalState().getState(maxDataValueState);

        String output_tag = null;
        System.out.println("Key::" + key);

        Double max = maxVal.value();
//
//
        System.out.println("Starting sum Value " + max);
        System.out.println(key + "->  start sum " + max);
//		System.out.println(key + "-> start samples" + samples);


        for(DataValue dataValue : iterable){
            System.out.println("PROCESSOR " + dataValue + " THREAD " + Thread.currentThread().getId());
            if (maxDataValue == null) {
                maxDataValue = new DataValue();
                maxDataValue.asset = dataValue.asset;
                maxDataValue.name = dataValue.name;
                maxDataValue.tag = dataValue.tag;
                maxDataValue.value = dataValue.value;
                output_tag = dataValue.tag;
                // FIXME: timestamp
            }
            if(max == null){
                max = dataValue.value;
            }
            if (max < dataValue.value){
                max = dataValue.value;
            }


// 			System.out.println("maxDataValue " +  maxDataValue);
        }

        System.out.println(key + "->  end sum " + max);
//		System.out.println(key + "-> end samples" + samples);
//
        System.out.println("Iterator End" + new Date());

        maxVal.update(max);

        if(maxVal.value() !=null ){


            maxDataValue = new DataValue();
            maxDataValue.value = maxVal.value();
            //maxDataValue.ts = dataValue.ts;
//			maxDataValue.setTag(maxDataValue.getOutput_tag());
            collector.collect(maxDataValue);
            System.out.println("DISPATCHING " + maxDataValue);
        }


    }
}