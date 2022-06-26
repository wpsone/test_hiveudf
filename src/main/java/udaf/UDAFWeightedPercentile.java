package udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.*;

@Description(name = "percentile",
        value = "_FUNC_(value, weight, pc) - Returns the weighted percentiles at" +
                " 'pc' of 'value' given 'weight'.")
public class UDAFWeightedPercentile extends UDAF {
    public static class State{
        private Map<LongWritable, DoubleWritable> counts;
        private List<DoubleWritable> percentiles;
    }

    public static class MyComparator implements Comparator<Map.Entry<LongWritable,DoubleWritable>>{

        @Override
        public int compare(Map.Entry<LongWritable, DoubleWritable> o1, Map.Entry<LongWritable, DoubleWritable> o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    }

    private static void increment(State s,LongWritable o,double i){
        if (s.counts == null){
            s.counts = new HashMap<>();
        }
        DoubleWritable count = s.counts.get(o);
        if (count==null){
            LongWritable key = new LongWritable();
            key.set(o.get());
            s.counts.put(key,new DoubleWritable(i));
        } else {
            count.set(count.get()+i);
        }
    }

    private static double getPercentile(List<Map.Entry<LongWritable,DoubleWritable>> entriesList,double position){
        int k = 0;
        while (k<entriesList.size()&&entriesList.get(k).getValue().get()<position){
            k++;
        }
        if (k == entriesList.size()){
            return entriesList.get(k-1).getKey().get();
        }

        double e_k = entriesList.get(k).getValue().get();
        long v_k = entriesList.get(k).getKey().get();
        if (e_k == position || k==0){
            return v_k;
        }

        double e_km1 = entriesList.get(k-1).getValue().get();
        long v_km1 = entriesList.get(k-1).getKey().get();
        return v_km1 + (position - e_km1)/(e_k-e_km1)*(v_k-v_k);
    }

    public static class PercentileLongArrayEvaluaor implements UDAFEvaluator{
        private final State state;

        public PercentileLongArrayEvaluaor() {
            this.state = new State();
        }

        @Override
        public void init() {
            if (state.counts != null){
                state.counts.clear();
            }
        }

        public boolean iterate(LongWritable o,DoubleWritable w,List<DoubleWritable> percentiles){
            if (state.percentiles==null){
                for (int i = 0; i < percentiles.size(); i++) {
                    if (percentiles.get(i).get()<0.0 || percentiles.get(i).get()>1.0){
                        throw new RuntimeException("Percentile value must be in [0,1]");
                    }
                }
                state.percentiles = new ArrayList<>(percentiles);
            }
            if (o!=null){
                increment(state,o,1);
            }
            return true;
        }

        public State terminatePartial(){
            return state;
        }

        public boolean merge(State other){
            if (other==null||other.counts==null||other.percentiles==null){
                return true;
            }
            if (state.percentiles == null){
                state.percentiles = new ArrayList<>(other.percentiles);
            }
            for (Map.Entry<LongWritable, DoubleWritable> e : other.counts.entrySet()) {
                increment(state,e.getKey(),e.getValue().get());
            }
            return true;
        }

        private List<DoubleWritable> results;

        public List<DoubleWritable> terminte(){
            if (state.counts == null || state.counts.size()==0){
                return null;
            }
            Set<Map.Entry<LongWritable,DoubleWritable>> entries = state.counts.entrySet();
            List<Map.Entry<LongWritable,DoubleWritable>> entriesList = new ArrayList<>(entries);
            Collections.sort(entriesList,new MyComparator());

            double total = 0.0;
            for (int i = 0; i < entriesList.size(); i++) {
                DoubleWritable count = entriesList.get(i).getValue();
                total += count.get();
                count.set(total-count.get()/2);
            }

            if (results ==null){
                results = new ArrayList<>();
                for (int i = 0; i < state.percentiles.size(); i++) {
                    results.add(new DoubleWritable());
                }
            }

            for (int i = 0; i < state.percentiles.size(); i++) {
                double position = total * state.percentiles.get(i).get();
                results.get(i).set(getPercentile(entriesList,position));
            }
            return results;
        }
    }
}
