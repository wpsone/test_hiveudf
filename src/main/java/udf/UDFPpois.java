package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "ppois",
    value = "_FUNC_(k,r) - Evaluate the likelihood of observing k given expected rate r")
public class UDFPpois extends UDF {
    public Double evaluate(Integer k,Double r){
        if (k==null||r==null||k<0||r<=0.0){
            return null;
        }
        Double result = Math.exp(-r);
        if (k==0){
            return result;
        }
        Double logSum = 0.0;
        Double logR = Math.log(r);
        for (int i = 0; i <= k; i++) {
            logSum = logSum + Math.log(i);
            result = result + Math.exp(i*logR-r-logSum);
        }
        return result;
    }
}
