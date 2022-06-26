package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udfltrim",
    value = "_FUNC_(s,chars) - Return the string s " +
            "with the characters in chars trimmed off " +
            "the left side.")
public class UDFLtrim extends UDF {
    public String evaluate(String s,String chars){
        if (s ==null){
            return null;
        }
        if (chars==null){
            return s;
        }
        int i = 0;
        boolean done = false;
        while (i<s.length() && !done){
            if (chars.indexOf(s.charAt(i))==-1){
                done = true;
            } else {
                i++;
            }
        }
        int j = s.length() - 1;
        if (i>j){
            return new String();
        }
        return s.substring(i,j+1);
    }
    public String evaluate(String s){
        return this.evaluate(s,"\n\t");
    }
}
