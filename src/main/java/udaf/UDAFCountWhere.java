package udaf;

import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.UDF;

public class UDAFCountWhere extends UDF {
    public static class UDAFCountWhereEvaluator implements UDAFEvaluator{
        Integer c = 0;
        public UDAFCountWhereEvaluator(){
            super();
            init();
        }

        @Override
        public void init() {
            c = 0;
        }

        public boolean iterate(Boolean ThisBool){
            if (ThisBool != null && ThisBool){
                c++;
            }
            return true;
        }

        public Integer terminatePartial(){
            return c;
        }

        public boolean merge(Integer c1){
            c += c1;
            return true;
        }

        public Integer terminate(){
            return c;
        }
    }
}
