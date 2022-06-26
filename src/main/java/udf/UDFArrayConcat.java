package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.ArrayList;
import java.util.List;

@Description(name = "udfarrayconcat",
    value = "_FUNC_(values) - Concatenates the array arguments")
public class UDFArrayConcat extends GenericUDF {
    ListObjectInspector arrayOI = null;
    ObjectInspectorConverters.Converter converter[];

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        converter = new ObjectInspectorConverters.Converter[objectInspectors.length];

        for (int i = 0; i < objectInspectors.length; ++i) {
            if (i == 0){
                arrayOI = (ListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(objectInspectors[i]);
            }
            converter[i] = ObjectInspectorConverters.getConverter(objectInspectors[i],arrayOI);
        }
        return arrayOI;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        ArrayList<Object> result_array = null;
        for (int i = 0; i < deferredObjects.length; i++) {
            List<?> array = (List<?>) converter[i].convert(deferredObjects[i].get());
            if (array == null){
                continue;
            }

            if (result_array == null){
                result_array = new ArrayList<Object>(array);
            } else {
                result_array.addAll(array);
            }
        }
        return new ArrayList<Object>(result_array);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return new String();
    }
}
