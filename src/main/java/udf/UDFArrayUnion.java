package udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class UDFArrayUnion extends GenericUDF {
    ListObjectInspector arrayOI = null;
    ObjectInspectorConverters.Converter converter[];

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        converter = new ObjectInspectorConverters.Converter[objectInspectors.length];
        for (int i = 0;i < objectInspectors.length; ++i) {
            if (i == 0){
                arrayOI = (ListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(objectInspectors[i]);
            }
        }
        return arrayOI;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        HashSet<Object> result_set = null;
        for (int i = 0; i < deferredObjects.length; i++) {
            List<?> array = (List<?>) converter[i].convert(deferredObjects[i].get());
            if (array == null){
                continue;
            }
            if (result_set == null){
                result_set = new HashSet<>(array);
            } else {
                result_set.addAll(array);
            }
        }
        if (result_set != null){
            return new ArrayList<Object>(result_set);
        } else {
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return new String();
    }
}
