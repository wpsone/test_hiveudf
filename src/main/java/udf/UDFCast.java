package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;

@Description(name = "udfcase",
    value = "_FUNC_(value1,value2) - Casts value1 so that it matches the type of value2.")
public class UDFCast extends GenericUDF {
    ObjectInspectorConverters.Converter converter;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        converter = ObjectInspectorConverters.getConverter(objectInspectors[0],objectInspectors[1]);
        return objectInspectors[1];
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        return converter.convert(deferredObjects[0].get());
    }

    @Override
    public String getDisplayString(String[] strings) {
        return new String();
    }
}
