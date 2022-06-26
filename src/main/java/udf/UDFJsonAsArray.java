package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

@Description(name = "udfjsonasarray",
    value = "_FUNC_(array_string) - Convert a string of a JSON-encoded array to a Hive array of strings.")
public class UDFJsonAsArray extends UDF {
    public  ArrayList<String> evaluate(String jsonString){
        if (jsonString == null){
            return null;
        }
        try {
            JSONArray extractObject = new JSONArray(jsonString);
            ArrayList<String> result = new ArrayList<>();
            for (int i = 0; i < extractObject.length(); i++) {
                result.add(extractObject.get(i).toString());
            }
            return result;
        } catch (JSONException e){
            return null;
        } catch (NumberFormatException e){
            return null;
        }
    }
}
