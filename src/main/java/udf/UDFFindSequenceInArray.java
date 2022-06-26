package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

@Description(name = "udffindsequenceinarray",
    value = "_FUNC_(NEEDLE,HAYSTACK) - Find the index where one array (the needle) first occurs as a subsequence of the other (the haystack).")
public class UDFFindSequenceInArray extends UDF {
    public Integer evaluate(ArrayList<String> needle,ArrayList<String> haystack) {
        if (needle == null){
            return Integer.valueOf(-1);
        }
        if (haystack == null){
            return null;
        }
        for (int i = 0; i < haystack.size() - needle.size() + 1; ++i) {
            boolean found = true;
            for (int j = 0; j < needle.size(); ++j) {
                if (haystack.get(i+j) == null ||
                    needle.get(j) == null ||
                    !haystack.get(i+j).equals(needle.get(j))){
                    found = false;
                    break;
                }
            }
            if (found){
                return Integer.valueOf(i);
            }
        }
        return Integer.valueOf(-1);
    }
}
