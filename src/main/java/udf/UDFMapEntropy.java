package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;

@Description(name = "udfmapentropy",
    value = "_FUNC_(histogram) - Return the noramlized entropy of the histogram")
public class UDFMapEntropy extends UDF {
    private static final double log2 = Math.log(2);
    public Double evaluate(HashMap<String,Double> histogram){
        if (histogram == null){
            return null;
        }

        double total = 0.0;
        for (Double value : histogram.values()) {
            if (value != null) {
                if (value != null){
                    if (value >=0){
                        total += value;
                    } else {
                        return null;
                    }
                }
            }
        }

        if (total == 0){
            return Double.valueOf(0.0);
        }

        double entropy = 0.0;
        for (Double value : histogram.values()) {
            if (value != null && value > 0){
                entropy -= (value/total)*Math.log(value/total);
            }
        }

        if (entropy < 0){
            entropy = 0;
        } else {
            entropy /= log2;
        }
        return Double.valueOf(entropy);
    }
}
