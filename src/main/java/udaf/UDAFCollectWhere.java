package udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;

import java.util.ArrayList;
import java.util.List;

@Description(name = "collect_where",
    value = "_FUNC_(value,condition) - aggregate the values which satisfy the condition into an array")
public class UDAFCollectWhere extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        return new Evaluator();
    }

    public static class State implements AggregationBuffer{
        ArrayList<Object> elements = new ArrayList<>();
    }

    public static class Evaluator extends GenericUDAFEvaluator{
        ObjectInspector inputOI;
        ListObjectInspector internalMergeOI;
        ObjectInspector conditionOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m,parameters);

            if (m == Mode.COMPLETE || m == Mode.PARTIAL1){
                inputOI = parameters[0];
                conditionOI = parameters[1];
                return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputOI));
            } else {
                internalMergeOI = (ListObjectInspector) parameters[0];
                return ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new State();
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((State)aggregationBuffer).elements.clear();
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            if (objects[0]!=null && objects[1]!=null){
                BooleanWritable condition = (BooleanWritable) ObjectInspectorUtils.copyToStandardObject(objects[1],conditionOI);
                if (condition.get()){
                    State state = (State) aggregationBuffer;
                    state.elements.add(ObjectInspectorUtils.copyToStandardObject(objects[0],inputOI));
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return ((State)aggregationBuffer).elements;
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            if (o!=null){
                State state = (State) aggregationBuffer;
                state.elements.addAll((List<?>)ObjectInspectorUtils.copyToStandardObject(o,internalMergeOI));
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            return ((State)aggregationBuffer).elements;
        }
    }
}
