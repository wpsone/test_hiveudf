package udf;

import groovy.json.StringEscapeUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFUnescape extends UDF {
    public String evaluate(String s){
        if (s==null){
            return null;
        }
        return StringEscapeUtils.unescapeJava(s);
    }
}
