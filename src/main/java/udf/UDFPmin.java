package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udfpmin",
    value = "_FUNC_(...) - Find the value of the smallest element.Unlike MIN,PMIN finds the row-size minimum element (rather than the column-wise).NULLs are ignored except when all arguments are NULL in which case NULL is returned.")
public class UDFPmin extends UDF {
    public Double evaluate(Double... args){
        Double minVal = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i]!=null){
                if (minVal==null||args[i]<minVal){
                    minVal=args[i];
                }
            }
        }
        return minVal;
    }
}
