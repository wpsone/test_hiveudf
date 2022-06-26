package udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class UDAFSumWhere extends UDAF {
    public static class UDAFCountWhereEvaluator implements UDAFEvaluator{
        Double s = null;

        public UDAFCountWhereEvaluator(){
            super();
            init();
        }

        @Override
        public void init() {
            s = null;
        }

        public boolean iterate(Double item,Boolean ThisBool){
            if (item!=null && ThisBool != null && ThisBool){
                if (s==null){
                    s = item;
                } else {
                    s += item;
                }
            }
            return true;
        }

        public Double terminatePartial(){
            return s;
        }

        public boolean merge(Double s2){
            if (s2!=null){
                if (s==null){
                    s=s2;
                } else {
                    s += s2;
                }
            }
            return true;
        }

        public Double terminate(){
            return s;
        }
    }
}
