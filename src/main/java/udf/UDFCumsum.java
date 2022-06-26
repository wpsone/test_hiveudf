package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;

@Description(name = "udfcumsum",
    value = "_FUNC_(VAL,KEYS...) - Computes a cumulative sum on the VAL column. Resets whenever KEYS... changes")
public class UDFCumsum extends UDF {
    Object previous_keys[] = null;
    Double running_sum;

    public Double evaluate(Double val,Object... keys){
        if (previous_keys == null || !Arrays.equals(previous_keys,keys)){
            running_sum = 0.0;
            previous_keys = keys.clone();
        }
        if (val!=null){
            running_sum += val;
        }
        return running_sum;
    }
}
