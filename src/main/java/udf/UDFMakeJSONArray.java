package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.List;

@Description(name = "make_json_array",
    value = "_FUNC_(array) - JSON-encode a Hive array.")
public class UDFMakeJSONArray extends GenericUDF {
    ObjectInspector inputOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        inputOI = objectInspectors[0];
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get()==null){
            return null;
        }
        List<?> array = (List<?>) ObjectInspectorUtils.copyToStandardObject(deferredObjects[0].get(),inputOI, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
        JSONArray json_array = new JSONArray(array);
        if (json_array==null){
            return null;
        }
        return json_array.toString();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return new String();
    }
}
