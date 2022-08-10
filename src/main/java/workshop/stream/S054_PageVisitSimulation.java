package workshop.stream;


import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

@SuppressWarnings("serial")
public class S054_PageVisitSimulation {

    static final String[] NAMES = {"user1", "user2", "user3", "user4", "user5", "user6"};
    static final String[] GENDERS = {"male", "female", "unknown"};
    static final String[] REGIONS = {"region1", "region2", "region3", "region4", "region5", "region6"};


    /** Continuously generates (name, grade). */
    public static class UserSource implements Iterator<Tuple2<String, String>>, Serializable {

        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, String> next() {
            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)], GENDERS[rnd.nextInt(GENDERS.length)]);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public static DataStream<Tuple2<String, String>> getSource(
                StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(
                    new ThrottledIterator<>(new UserSource(), rate),
                    TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));
        }
    }

    /** Continuously generates (name, salary). */
    public static class PageSource implements Iterator<Tuple2<String, String>>, Serializable {

        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, String> next() {
            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)], REGIONS[rnd.nextInt(REGIONS.length)]);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public static DataStream<Tuple2<String, String>> getSource(
                StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(
                    new ThrottledIterator<>(new PageSource(), rate),
                    TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));
        }
    }
}