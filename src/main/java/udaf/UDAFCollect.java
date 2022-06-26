package udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.LinkedList;

public class UDAFCollect extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length!=1){
            throw new UDFArgumentTypeException(info.length - 1,"Exactly one argument is expected.");
        }
        return new GenericUDAFCollectEvaluator();
    }

    public static class GenericUDAFCollectEvaluator extends GenericUDAFEvaluator{
        ObjectInspector inputOI;
        ObjectInspector outputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length==1);
            super.init(m,parameters);
            inputOI = parameters[0];
            outputOI = ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputOI));
            return outputOI;
        }

        public static class UDAFCollectState implements AggregationBuffer{
            private LinkedList<Object> elements;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            UDAFCollectState myAgg = new UDAFCollectState();
            myAgg.elements = new LinkedList<Object>();
            return myAgg;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            UDAFCollectState myAgg = (UDAFCollectState) aggregationBuffer;
            myAgg.elements.clear();
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            UDAFCollectState myAgg = (UDAFCollectState) aggregationBuffer;
            assert (objects.length==1);
            if (objects[0]!=null){
                Object pCopy = ObjectInspectorUtils.copyToStandardObject(objects[0],inputOI);
                myAgg.elements.add(pCopy);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            UDAFCollectState myAgg = (UDAFCollectState) aggregationBuffer;
            if (myAgg.elements.size() == 0){
                return null;
            } else {
                return myAgg;
            }
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            if (o!=null){
                UDAFCollectState myAgg = (UDAFCollectState) aggregationBuffer;
                UDAFCollectState myPartial = (UDAFCollectState) o;
                myAgg.elements.addAll(myPartial.elements);
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            UDAFCollectState myAgg = (UDAFCollectState) aggregationBuffer;
            return myAgg.elements;
        }
    }
}
