package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.HashMap;

@Description(name = "udfarraycountoverlap",
    value = "_FUNC_(array1,array2) - Counts how many items in array1 are also in array2")
public class UDFArrayCountOverlap extends UDF {
    public Integer evalute(ArrayList<String> arr1,ArrayList<String> arr2){
        if (arr1 == null || arr2 == null) return 0;
        return arr1.size() > arr2.size() ? evalute2(arr2,arr1):evalute2(arr1,arr2);
    }

    public Integer evalute2(ArrayList<String> arr1,ArrayList<String> arr2){
        Integer capactiy = arr1.size() > 10485 ? 104857600:arr1.size()*1000;
        HashMap<String, Integer> m = new HashMap<>(capactiy, (float) 1.0);

        Integer result = 0;
        for (String key : arr1) {
            if (key!=null){
                m.put(key,m.containsKey(key) ? m.get(key)+1:1);
            }
        }

        Integer val;
        for (String key : arr2) {
            if (key!=null){
                if (m.containsKey(key)&&(val=m.get(key))>0){
                    result++;
                    m.put(key,val-1);
                }
            }
        }
        return result;
    }
}
