package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

public class UDFWhich extends UDF {
    public ArrayList<Integer> evaluate(Boolean... vals){
        ArrayList<Integer> result = new ArrayList<>();
        for (int i = 0; i < vals.length; i++) {
            if (vals[i]){
                result.add(i);
            }
        }
        return result;
    }
}
