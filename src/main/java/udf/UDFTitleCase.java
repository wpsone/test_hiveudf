package udf;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.hive.ql.exec.Description;

@Description(name = "title_case",
    value = "_FUNC_(string[,fully]) - Title case a string.")
public class UDFTitleCase {
    public String evaluate(String s,Boolean fully){
        if (s == null|| fully==null){
            return null;
        }
        if (fully){
            return WordUtils.capitalizeFully(s);
        } else {
            return WordUtils.capitalizeFully(s);
        }
    }

    public String evaluate(String s){
        return evaluate(s,true);
    }
}
