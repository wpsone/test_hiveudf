package udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class UDAFTop extends UDAF {
    public static class UDAFTopState{
        private double value;
        private String key;
    }

    public static class UDAFTopEvaluator implements UDAFEvaluator{
        UDAFTopState state;

        public UDAFTopEvaluator(){
            super();
            state = new UDAFTopState();
            init();
        }

        @Override
        public void init() {
            state.value = Double.NEGATIVE_INFINITY;
            state.key = null;
        }

        public boolean iterate(String key,Double value){
            if (value!=null && value > state.value){
                state.value = value;
                state.key =key;
            }
            return true;
        }

        public UDAFTopState terminatePartial(){
            return state;
        }

        public boolean merge(UDAFTopState o){
            if (o!=null){
                if (o.value> state.value){
                    state.value = o.value;
                    state.key = o.key;
                }
            }
            return true;
        }

        public String terminate(){
            return state.key;
        }
    }
    private UDAFTop(){

    }
}
