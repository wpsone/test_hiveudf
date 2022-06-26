package udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class UDAFGroupLongest extends UDAF {
    public static class UDAFLongestState {
        private String longestString;
    }

    public static class UDAFLongestEvaluator implements UDAFEvaluator {
        UDAFLongestState state;

        public UDAFLongestEvaluator(){
            super();
            state = new UDAFLongestState();
            init();
        }

        @Override
        public void init() {
            state.longestString = null;
        }

        public boolean iterate(String str){
            if (str!=null && (state.longestString == null || str.length() > state.longestString.length())){
                state.longestString = new String(str);
            }
            return true;
        }

        public UDAFLongestState terminatePartial(){
            return state;
        }

        public boolean merge(UDAFLongestState o){
            if (o!=null && o.longestString !=null){
                if (state.longestString ==null ||
                        o.longestString.length() > state.longestString.length()){
                    state.longestString = o.longestString;
                }
            }
            return true;
        }

        public String terminate(){
            return state.longestString;
        }
    }
    private UDAFGroupLongest(){

    }

}
