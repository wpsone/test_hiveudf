package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@Description(name = "udfmapexclude",
    value = "_FUNC_(values,indices) - Removes elements of 'values'" +
            " whose keys are in 'indices'")
public class UDFMapExclude extends GenericUDF {
    ListObjectInspector arrayOI;
    MapObjectInspector mapOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        mapOI = (MapObjectInspector) objectInspectors[0];
        arrayOI = (ListObjectInspector) objectInspectors[1];
        ObjectInspector mapItemOI = mapOI.getMapKeyObjectInspector();
        ObjectInspector listItemOI = arrayOI.getListElementObjectInspector();

        if (!ObjectInspectorUtils.compareTypes(mapItemOI,listItemOI)){
            throw new UDFArgumentException("Map key type ("+mapItemOI+") must match " +
                    "list element type ("+listItemOI+").");
        }
        return ObjectInspectorUtils.getStandardObjectInspector(mapOI, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
     }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Map<?,?> value_map = (Map<?,?>)ObjectInspectorUtils.copyToStandardObject(deferredObjects[0].get(),mapOI, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
        List<?> index_array = (List<?>)ObjectInspectorUtils.copyToStandardObject(deferredObjects[1].get(),arrayOI, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);

        if (value_map == null || index_array == null){
            return null;
        }
        HashSet<Object> indices = new HashSet<>(index_array);
        HashMap<Object,Object> result = new HashMap<>();
        for (Object key : value_map.keySet()) {
            if (!indices.contains(key)){
                result.put(key,value_map.get(key));
            }
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return new String();
    }
}
