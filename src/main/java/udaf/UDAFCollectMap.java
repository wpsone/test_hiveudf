package udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.HashMap;

public class UDAFCollectMap extends UDAF {
    public static class UDAFCollectMapState{
        private HashMap<String,String> elements;
    }

    public static class UDAFCollectMapEvaluator implements UDAFEvaluator{
        UDAFCollectMapState state;

        public UDAFCollectMapEvaluator(){
            super();
            state = new UDAFCollectMapState();
            init();
        }

        @Override
        public void init() {
            state.elements = new HashMap<>();
        }

        public boolean iterate(String key,String val){
            if (key != null && val != null){
                state.elements.put(key,val);
            }
            return true;
        }

        public UDAFCollectMapState terminatePartial(){
            if (state.elements.size() == 0){
                return null;
            } else {
                return state;
            }
        }

        public boolean merge(UDAFCollectMapState o){
            if (o!=null){
                state.elements.putAll(o.elements);
            }
            return true;
        }

        public HashMap<String,String> terminate(){
            return state.elements;
        }
    }

    private UDAFCollectMap(){

    }
}
