package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.HashSet;

@Description(name = "udfarrayintersect",
    value = "_FUNC_(values) - Computes the intersection of the array arguments. Note that ordering will be lost.")
public class UDFArrayIntersect extends UDF {
    public ArrayList<String> evaluate(ArrayList<String>... arrays){
        HashSet<String> result_set = null;
        for (int i = 0; i < arrays.length; i++) {
            if (arrays[i]==null){
                continue;
            }
            if (result_set == null){
                result_set = new HashSet<String>(arrays[i]);
            } else {
                result_set.retainAll(arrays[i]);
            }
        }
        return new ArrayList<String>(result_set);
    }
}
