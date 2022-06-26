package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Description(name = "udfsample",
    value = "_FUNC_(N,A) - Randomly samples (at most) N elements from array A.")
public class UDFSample extends GenericUDF {
    private ObjectInspectorConverters.Converter int_converter;
    private ListObjectInspector arrayOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2){
            throw new UDFArgumentLengthException("SAMPLE expects two arguments.");
        }
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"SMPLE expects an INTEGER as its first arguments");
        }
        int_converter = ObjectInspectorConverters.getConverter(objectInspectors[0], PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        arrayOI = (ListObjectInspector) objectInspectors[1];
        return objectInspectors[1];
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        IntWritable intWritable = (IntWritable) int_converter.convert(deferredObjects[0].get());
        if (intWritable == null){
            return null;
        }
        int N = intWritable.get();
        if (N<0){
            throw new UDFArgumentException("SAMPLE requires a nonegative number of elements to sample.");
        }
        List<?> array = arrayOI.getList(deferredObjects[1].get());
        if (array==null){
            return null;
        }
        if (N>=array.size()){
            return deferredObjects[1].get();
        }
        ArrayList<Object> array_copy = new ArrayList<>(array);
        Collections.shuffle(array_copy);
        return array_copy.subList(0,N);
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == 2);
        StringBuilder sb = new StringBuilder();
        sb.append("fb_sample")
                .append(strings[0])
                .append(",")
                .append(strings[1])
                .append(")");
        return sb.toString();
    }
}
