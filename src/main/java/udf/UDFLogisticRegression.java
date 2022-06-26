package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udflogisticregression",
    value = "_FUNC_(values) - Randomly samples from an 0-indexed index with weight proportional to arguments.")
public class UDFLogisticRegression extends UDF {
    public Integer evaluate(Double... vals){
        double sum = 0.0;
        for (int i = 0; i < vals.length; i++) {
            if (vals[i]==null){
                return null;
            }
            sum += vals[i];
        }
        double r = Math.random();
        for (int i = 0; i < vals.length; i++) {
            if (r<vals[i]/sum){
                return Integer.valueOf(i);
            }
            r -= vals[i]/sum;
        }
        assert false;
        return null;
    }
}
