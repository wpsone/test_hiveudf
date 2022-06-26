package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udfargmax",
    value = "_FUNC_(double,double,...) - Find the index with the largest value",
    extended = "Example:\n > SELECT ARGMAX(foo,bar) FROM users;\n")
public class UDFArgMax extends UDF {
    public Integer evaluate(Double... args){
        Integer which_max = null;
        Double max_val = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < args.length; ++i) {
            if (args[i]!=null && args[i]>max_val){
                max_val = args[i];
                which_max = i;
            }
        }
        return which_max;
    }
}
