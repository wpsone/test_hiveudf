package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "ends_with",
    value = "_FUNC_(haystack,needls)")
public class UDFEndsWith extends UDF {
    public Boolean evaluate(String haystack,String needle){
        if (haystack == null || needle == null){
            return null;
        }
        return haystack.endsWith(needle);
    }
}
