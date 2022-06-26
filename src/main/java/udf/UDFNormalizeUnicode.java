package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.Normalizer;

@Description(name = "udfnormalizeunicode",
    value = "_FUNC_(string,form) - Normalization the unicode 'string' to 'from'.")
public class UDFNormalizeUnicode extends UDF {
    public String evaluate(String s,String form){
        if (s==null||form==null){
            return null;
        }
        if (form.equals("NFC")){
            return Normalizer.normalize(s,Normalizer.Form.NFC);
        } else if (form.equals("NFD")){
            return Normalizer.normalize(s,Normalizer.Form.NFD);
        } else if (form.equals("NFKC")){
            return Normalizer.normalize(s,Normalizer.Form.NFKC);
        } else if (form.equals("NFKD")){
            return Normalizer.normalize(s,Normalizer.Form.NFKD);
        } else {
            return null;
        }
    }
}
