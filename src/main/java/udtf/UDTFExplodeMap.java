package udtf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

@Description(name = "explode_map",
    value = "_FUNC_(a) - separates the elements of map a into multiple rows")
public class UDTFExplodeMap extends GenericUDTF {
    private MapObjectInspector mapOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length!=1){
            throw new UDFArgumentException("explode_map() takes only one argument");
        }
        if (argOIs[0].getCategory() != ObjectInspector.Category.MAP){
            throw new UDFArgumentException("explode_map() takes a map as a parameter");
        }
        mapOI = (MapObjectInspector) argOIs[0];

        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("col1");
        fieldNames.add("col2");
        fieldOIs.add(mapOI.getMapKeyObjectInspector());
        fieldOIs.add(mapOI.getMapValueObjectInspector());
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    private final Object[] forwardObj = new Object[2];

    @Override
    public void process(Object[] objects) throws HiveException {
        Map<?,?> map = mapOI.getMap(objects[0]);
        if (map==null){
            return;
        }
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            forwardObj[0] = entry.getKey();
            forwardObj[1] = entry.getValue();
            forward(forwardObj);
        }
    }

    @Override
    public void close() throws HiveException {

    }

    @Override
    public String toString() {
        return "UDTFExplodeMap{" +
                "mapOI=" + mapOI +
                ", forwardObj=" + Arrays.toString(forwardObj) +
                '}';
    }
}
