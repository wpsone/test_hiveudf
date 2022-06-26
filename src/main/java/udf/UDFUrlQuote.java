package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class UDFUrlQuote extends UDF {
    public String evaluate(String s) throws UnsupportedEncodingException {
        if (s==null){
            return null;
        }
        return URLEncoder.encode(s,"UTF-8");
    }
}
