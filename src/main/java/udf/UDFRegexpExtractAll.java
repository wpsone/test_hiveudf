package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.LinkedList;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Description(name = "regexp_extract_all",
    value = "_FUNC_(haystack,pattern,[index]) - Find all the instances of pattern in haystack.")
public class UDFRegexpExtractAll extends UDF {
    private String lastRegex = null;
    private Pattern p = null;
    public LinkedList<String> evaluate(String s,String regex,Integer extractIndex){
        if (s==null||regex==null||extractIndex==null){
            return null;
        }
        if (!regex.equals(lastRegex)){
            lastRegex = regex;
            p = Pattern.compile(regex,Pattern.MULTILINE);
        }
        LinkedList<String> result = new LinkedList<>();
        Matcher m = p.matcher(s);
        while (m.find()){
            MatchResult mr = m.toMatchResult();
            result.add(mr.group(extractIndex));
        }
        return result;
    }
    public LinkedList<String> evaluate(String s,String regex){
        return this.evaluate(s,regex,0);
    }
}
