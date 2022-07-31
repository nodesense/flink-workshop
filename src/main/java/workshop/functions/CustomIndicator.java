package workshop.functions;

import org.ta4j.core.Bar;
import org.ta4j.core.BarSeries;
import org.ta4j.core.Indicator;
import org.ta4j.core.indicators.CachedIndicator;
import org.ta4j.core.num.DoubleNum;
import org.ta4j.core.num.Num;

import java.util.Vector;
import java.util.function.Function;


public class CustomIndicator implements Indicator<Num> {
    public CustomIndicator() {

    }

    Vector<Num> numbers = new Vector<>();

    public void addNum(Num value) {
        numbers.add(value);
    }

    @Override
    public Num getValue(int i) {
        return numbers.get(i);
    }

    @Override
    public BarSeries getBarSeries() {
        return null;
    }

    @Override
    public Num numOf(Number number) {
        return   DoubleNum.valueOf(number);
    }

}
