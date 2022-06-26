package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONObject;

import java.util.Map;

@Description(name = "make_json_obj",
    value = "_FUNC_(map) - JSON encode a Hive map.")
public class UDFMakeJSONObj extends GenericUDF {
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
        Map<?,?> map = (Map<?, ?>) ObjectInspectorUtils.copyToStandardObject(deferredObjects[0].get(),inputOI, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
        JSONObject json_object = new JSONObject(map);
        if (json_object==null){
            return null;
        }
        return json_object.toString();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return new String();
    }
}
