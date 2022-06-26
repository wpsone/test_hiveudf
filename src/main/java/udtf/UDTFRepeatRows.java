package udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;

public class UDTFRepeatRows extends GenericUDTF {
    private PrimitiveObjectInspector colOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        if (argOIs.length != 1){
            throw new UDFArgumentException("repeat_rows() requires exactly one argument");
        }
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentException("repeat_rows() expects an integer argument");
        }
        colOI = (PrimitiveObjectInspector) argOIs[0];
        if (colOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT){
            throw new UDFArgumentException("repeat_rows() expects an integer argument");
        }
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("col0");
        fieldOIs.add(colOI);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        Integer val = (Integer) colOI.getPrimitiveJavaObject(objects[0]);
        if (val==null){
            return;
        }
        if (val<0){
            throw new HiveException("repeat_rows() expects a non-negative argument");
        }
        for (int i = 0; i < val; i++) {
            forward(objects);
        }
    }

    @Override
    public void close() throws HiveException {

    }

    @Override
    public String toString() {
        return "UDTFRepeatRows{" +
                "colOI=" + colOI +
                '}';
    }
}
