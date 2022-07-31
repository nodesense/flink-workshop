package workshop.functions;

import workshop.models.DataValue;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class MaxFunction  extends KeyedProcessFunction<String, DataValue, DataValue> {

    private ValueStateDescriptor<DataValue> maxDataValueState = new ValueStateDescriptor("MinSumDataValueState",DataValue.class);

    public MaxFunction() {

    }

    private static final long serialVersionUID = 1L;


    @Override
    public void processElement(
            DataValue dataValue ,
            Context context,
            Collector<DataValue> collector) throws Exception {


        System.out.println("Iterator Begin " + new Date());
        ValueState<DataValue> maxVal =  getRuntimeContext().getState(maxDataValueState);

        DataValue max = maxVal.value();

        System.out.println("Starting sum Value " + max);



            System.out.println("PROCESSOR " + dataValue + " THREAD " + Thread.currentThread().getId());

            if(max == null){
                max = dataValue;
                maxVal.update(max);
                collector.collect(max); // first time value since no value earlier, we sent..
            }

            if (max.value < dataValue.value){
                max = dataValue;
                maxVal.update(max);
                collector.collect(max); // only max values are sent
            }

            // collector.collect(max);  // last known max value result send for every input record





    }

}