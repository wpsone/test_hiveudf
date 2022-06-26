package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udflongest",
    value = "_FUNC_(string...) - Return the longest string.")
public class UDFLongest extends UDF {
    public String evaluate(String... strs){
        String longest = null;
        for (int i = 0; i < strs.length; i++) {
            if (strs[i]==null){
                continue;
            }
            if (longest == null||strs[i].length()>longest.length()){
                longest = strs[i];
            }
        }
        if (longest == null){
            return null;
        } else {
            return new String(longest);
        }
    }
}
