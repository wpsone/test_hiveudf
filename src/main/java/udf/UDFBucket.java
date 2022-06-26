package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

@Description(name = "udfbucket",
    value = "_FUNC_(double,...) - Find the buckets the first argument belongs to",
    extended = "Example:\n >  BUCKET(foo,0,1,2) FROM users;\n")
public class UDFBucket extends UDF {
    public Integer evaluate(Double value,Double... buckets){
        if (value == null){
            return null;
        }
        for (int i = 0; i < buckets.length; i++) {
            if (value <= buckets[i]){
                return Integer.valueOf(i);
            }
        }
        return Integer.valueOf(buckets.length);
    }

    public Integer evaluate(Double value, ArrayList<Double> buckets){
        if (value == null){
            return null;
        }
        for (int i = 0; i < buckets.size(); i++) {
            if (value<=buckets.get(i)){
                return Integer.valueOf(i);
            }
        }
        return Integer.valueOf(buckets.size());
    }
}
