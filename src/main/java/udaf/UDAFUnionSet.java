package udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@Description(
        name = "union_set",
        value = "_FUNC_(col) - aggregate the values of an array column to one array",
        extended = "Aggregate the values, return as an ArrayList.")
public class UDAFUnionSet extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        return new Evaluator();
    }

    public static class State implements AggregationBuffer{
        HashSet<Object> set = new HashSet<>();
    }

    public static class Evaluator extends GenericUDAFEvaluator{
        ObjectInspector inputOI;
        ListObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m,parameters);
            if (m==Mode.COMPLETE|| m==Mode.PARTIAL1){
                inputOI = parameters[0];
            } else {
                internalMergeOI = (ListObjectInspector) parameters[0];
            }
            return ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new State();
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((State)aggregationBuffer).set.clear();
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            if (objects[0]!=null){
                State state = (State) aggregationBuffer;
                state.set.addAll((List<?>)ObjectInspectorUtils.copyToStandardObject(objects[0],inputOI));
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return new ArrayList<>(((State)aggregationBuffer).set);
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            if (o!=null){
                State state = (State) aggregationBuffer;
                List<?> pset = (List<?>)ObjectInspectorUtils.copyToStandardObject(o,internalMergeOI);
                state.set.addAll(pset);
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            return new ArrayList<>(((State)aggregationBuffer).set);
        }
    }
}
