package udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class UDAFAll extends UDAF {
    public static class UDAFAllEvaluator implements UDAFEvaluator {
        Boolean result = null;
        Boolean any_rows_seen = false;

        public UDAFAllEvaluator(){
            super();
            init();
        }

        @Override
        public void init() {
            result = null;
            any_rows_seen = false;
        }

        public boolean iterate(Boolean ThisBool){
            if (result!=null && !result){
                ;
            } else if (ThisBool ==null){
                result = null;
            } else if (!ThisBool){
                result = false;
            } else if (!any_rows_seen){
                result = true;
            } else {
                ;
            }
            any_rows_seen = true;
            return true;
        }

        public Boolean terminatePartial(){
            return result;
        }

        public Boolean merge(Boolean soFar){
            iterate(soFar);
            return true;
        }
        public Boolean terminate(){
            return result;
        }
    }
}
