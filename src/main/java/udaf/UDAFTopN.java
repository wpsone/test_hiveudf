package udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.LinkedList;

public class UDAFTopN extends UDAF {
    public static class StringDoublePair implements Comparable<StringDoublePair>{
        private String key;
        private Double value;

        public StringDoublePair(){
            this.key = null;
            this.value = null;
        }

        public StringDoublePair(String key,double value){
            this.key = key;
            this.value = value;
        }

        public String getString(){
            return this.key;
        }


        @Override
        public int compareTo(StringDoublePair o) {
            return o.value.compareTo(this.value);
        }

        public static class UDAFTopNState{
            private LinkedList<StringDoublePair> queue;
            private Integer N;
        }



    }

    public static class UDAFTopNEvaluator implements UDAFEvaluator{
        StringDoublePair.UDAFTopNState state;

        public UDAFTopNEvaluator(){
            super();
            state = new StringDoublePair.UDAFTopNState();
            init();
        }

        @Override
        public void init() {
            state.queue = new LinkedList<>();
            state.N = null;
        }

        void prune(LinkedList<StringDoublePair> queue,int N){
            while (queue.size()>N){
                queue.removeLast();
            }
        }

        public boolean iterate(String key,Double value,Integer N){
            if (state.N == null){
                state.N = N;
            }
            if (value != null){
                state.queue.add(new StringDoublePair(key,value));
                prune(state.queue, state.N);
            }
            return true;
        }

        public StringDoublePair.UDAFTopNState terminatePartial(){
            if (state.queue.size()>0){
                return state;
            } else {
                return null;
            }
        }

        public boolean merge(StringDoublePair.UDAFTopNState o){
            if (o!=null){
                state.queue.addAll(o.queue);
                if (o.N!=state.N){

                }
                prune(state.queue, state.N);
            }
            return true;
        }

        public LinkedList<String> terminate(){
            LinkedList<String> result = new LinkedList<>();
            while (state.queue.size() > 0){
                StringDoublePair p = state.queue.poll();
                result.addFirst(p.getString());
            }
            return result;
        }
    }
}
