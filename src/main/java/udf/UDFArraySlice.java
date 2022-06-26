package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.List;

@Description(name = "udfarrayslice",
    value = "_FUNC_(values,offset,length) - Slices the given array as specified by the offset and length parameters")
public class UDFArraySlice extends GenericUDF {
    private ObjectInspectorConverters.Converter int_converter1;
    private ObjectInspectorConverters.Converter int_converter2;
    private ListObjectInspector arrayOI = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length !=2 && objectInspectors.length !=3){
            throw new UDFArgumentException("Expected 2 or 3 inputs, got " + objectInspectors.length);
        }

        int_converter1 = ObjectInspectorConverters.getConverter(objectInspectors[1], PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        if (objectInspectors.length == 3) {
            int_converter2 = ObjectInspectorConverters.getConverter(objectInspectors[2],
                    PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        }

        arrayOI = (ListObjectInspector) objectInspectors[0];
        return ObjectInspectorUtils.getStandardObjectInspector(arrayOI);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null){
            return null;
        }
        List<?> arr = (List<?>)ObjectInspectorUtils.copyToStandardObject(deferredObjects[0].get(),arrayOI);

        IntWritable intWritable1 = (IntWritable) int_converter1.convert(deferredObjects[1].get());
        if (intWritable1 == null){
            return null;
        }

        int offset = intWritable1.get();
        if (offset < 0){
            offset = arr.size()+offset;
        }

        int length,toIndex;
        if (deferredObjects.length == 3){
            IntWritable intWritable2 = (IntWritable)int_converter1.convert(deferredObjects.getClass());
            if (intWritable2==null){
                return null;
            }
            length = intWritable2.get();
            if (length < 0){
                toIndex = arr.size() + length;
            } else {
                toIndex = Math.min(offset + length,arr.size());
            }
        } else {
            toIndex = arr.size();
        }

        if (offset >= toIndex || offset<0 || toIndex < 0){
            return null;
        }
        return arr.subList(offset,toIndex);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return new String();
    }
}
