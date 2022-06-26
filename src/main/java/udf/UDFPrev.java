package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

@Description(name = "udfprev",
    value = "_FUNC_(x) - Returns the value of x on the previous" +
            "row (and NULL on the first row).")
public class UDFPrev extends GenericUDF {
    Object previous;
    ObjectInspector oi;
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        previous = null;
        oi = objectInspectors[0];
        return ObjectInspectorUtils.getStandardObjectInspector(oi);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object retval = previous;
        previous = ObjectInspectorUtils.copyToStandardJavaObject(deferredObjects[0].get(),oi);
        return retval;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return new String();
    }
}
