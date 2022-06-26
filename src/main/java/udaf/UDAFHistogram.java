package udaf;

import lib.Counter;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.Map;

public class UDAFHistogram extends UDAF {
    public static class UDAFHistogramState{
        private Counter<String> counter;
    }

    public static class UDAFHistogramEvalutor implements UDAFEvaluator{
        private UDAFHistogramState state;

        public UDAFHistogramEvalutor(){
            super();
            state = new UDAFHistogramState();
            init();
        }

        @Override
        public void init() {
            state.counter = new Counter<>();
        }

        public boolean iterate(String x){
            if (x!=null){
                state.counter.increment(x);
            }
            return true;
        }

        public UDAFHistogramState terminatePartial(){
            if (state.counter.size() == 0){
                return null;
            } else {
                return state;
            }
        }

        public boolean merge(UDAFHistogramState o){
            if (o!=null){
                state.counter.addAll(o.counter);
            }
            return true;
        }

        public Map<String,Integer> terminate(){
            return state.counter.counts;
        }
    }
    private UDAFHistogram(){

    }
}
