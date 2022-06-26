package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udfpsum",
    value = "_FUNC_(...) - Find the sum of arguments passed.Unlike SUM,PSUM adds up all of its arguments row-wide,rather than adding up a column of data.If any element is NULL,NULL is returned.")
public class UDFPsum extends UDF {
    public Double evaluate(Double... args){
        Double psum = args.length > 0 ? 0.0 : null;
        for (int i = 0; i < args.length; i++) {
            if (args[i]==null){
                return null;
            }
            psum += args[i];
        }
        return psum;
    }
}
