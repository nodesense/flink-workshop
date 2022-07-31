package workshop.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

/**
 * This example shows an implementation of WordCount without using the Tuple2 type, but a custom
 * class.
 */
@SuppressWarnings("serial")
public class S012_WordCountPojo {

    /**
     * This is the POJO (Plain Old Java Object) that is being used for all the operations. As long
     * as all fields are public or have a getter/setter, the system can handle them
     */
    public static class Word {

        // fields
        private String word;
        private int frequency;

        // constructors
        public Word() {}

        public Word(String word, int i) {
            this.word = word;
            this.frequency = i;
        }

        @Override
        public String toString() {
            return "Word=" + word + " freq=" + frequency;
        }
    }

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        // get input data
        DataSet<String> text = env.fromElements(new String[]{
                "To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,",
                "And by opposing end them?--To die,--to sleep,--",});


        DataSet<Word> counts =
                // split up the lines into Word objects (with frequency = 1)
                text.flatMap(new Tokenizer())
                        // group by the field word and sum up the frequency
                        .groupBy(new KeySelector<Word, String>() {
                            public String getKey(Word  word) {
                                return word.word;
                            }
                        })
                        .reduce(
                                new ReduceFunction<Word>() {
                                    @Override
                                    public Word reduce(Word value1, Word value2) throws Exception {
                                        return new Word(
                                                value1.word, value1.frequency + value2.frequency);
                                    }
                                });

            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();

    }


    public static final class Tokenizer implements FlatMapFunction<String, Word> {

        @Override
        public void flatMap(String value, Collector<Word> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Word(token, 1));
                }
            }
        }
    }
}