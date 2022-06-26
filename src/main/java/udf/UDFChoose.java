package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "choose",
    value = "_FUNC_(v1,v2,...) - Randomly samples an element from a 0-indexed with weight proportional to arguments.")
public class UDFChoose extends UDF {
    public Integer evaluate(Double... vals){
        double sum = 0.0;
        for (int i = 0; i < vals.length; ++i) {
            Double v = vals[i];
            if (v == null || v < 0 || v.isNaN() || v.isInfinite()){
                return null;
            }
            sum += vals[i];
        }

        double r = Math.random() * sum;
        for (int i = 0; i < vals.length; ++i) {
            if (r < vals[i]){
                return Integer.valueOf(i);
            }
            r -= vals[i];
        }
        return Integer.valueOf(vals.length-1);
    }
}
