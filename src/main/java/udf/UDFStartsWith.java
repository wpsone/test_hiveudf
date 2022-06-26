package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "starts_with",
    value = "_FUNC_(haystack,needle) - Return whether " +
            "haystack begins with needle.")
public class UDFStartsWith extends UDF {
    public Boolean evaluate(String haystack,String needle){
        if (haystack == null||needle==null){
            return null;
        }
        return haystack.startsWith(needle);
    }
}
