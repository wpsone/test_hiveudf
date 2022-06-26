package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udfargmin",
    value = "_FUNC_(double,double,...) - Find the index with the samllest value ")
public class UDFArgMin extends UDF {
    public Integer evaluate(Double... args){
        Integer which_min = null;
        Double min_val = Double.POSITIVE_INFINITY;
        for (int i = 0; i < args.length; ++i) {
            if (args[i]!=null && args[i]<min_val){
                min_val = args[i];
                which_min = i;
            }
        }
        return which_min;
    }
}
