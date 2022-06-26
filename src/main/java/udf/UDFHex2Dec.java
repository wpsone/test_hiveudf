package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udfhex2dec",
    value = "_FUNC_(string) - Convert hex (string form) to decimal",
    extended = "Example:\n >SELECT HEX2DEC('D') FROM hex;\n")
public class UDFHex2Dec extends UDF {
    public Long evaluate(String hex_string){
        try {
            return Long.valueOf(hex_string,16).longValue();
        } catch (NumberFormatException e){
            return null;
        }
    }
}
