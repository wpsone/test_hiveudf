package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udftrim",
    value = "_FUNC_(s,chars) - Return the string s " +
            "with the characters in chars trimmed off the right.")
public class UDFRtrim extends UDF {
    public String evaluate(String s,String chars){
        if (s==null){
            return null;
        }
        if (chars==null){
            return s;
        }
        int i = 0;
        int j = s.length() - 1;
        boolean done = false;
        while (j >= i && !done){
            if (chars.indexOf(s.charAt(j))==-1){
                done = true;
            } else {
                j--;
            }
        }
        if (j<i){
            return new String();
        }
        return s.substring(i,j+1);
    }

    public String evaluate(String s){
        return this.evaluate(s,"\n\t");
    }
}
