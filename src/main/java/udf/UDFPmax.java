package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udfpmax",
    value = "_FUNC_(...) - Find the value of the largest element. Unlike MAX,PMAX finds the row-size maximum element (rather than the column-wise). NULLs are ignored except when all arguments are NULL in which case NULL is returned.",
    extended = "Example:\n > SELECT BUCKET(foo,bar) FROM users;\n")
public class UDFPmax extends UDF {
    public Double evaluate(Double... args){
        Double maxVal = null;
        for (int i = 0; i < args.length; i++) {
            if(args[i]!=null){
                if (maxVal==null || args[i]>maxVal){
                    maxVal = args[i];
                }
            }
        }
        return maxVal;
    }
}
