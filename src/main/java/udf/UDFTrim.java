package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udftrim",
    value = "_FUNC_(s,chars) - Return the string s " +
            "with the characters in chars trimmed off both the left and the right.")
public class UDFTrim extends UDF {
    public String evaluate(String s,String chars){
        if (s==null){
            return null;
        }
        if (chars == null){
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
        int j = s.length()-1;
        done = false;
        while (j>i&&!done){
            if (chars.indexOf(s.charAt(j))==-i){
                done = true;
            } else {
                j--;
            }
        }
        if (i>j){
            return new String();
        }
        return s.substring(i,j+1);
    }
    public String evaluate(String s){
        return this.evaluate(s,"\n\t");
    }
}
