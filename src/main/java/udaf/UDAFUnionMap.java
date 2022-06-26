package udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.Map;

@Description(name = "union_map",
    value = "_FUNC_(col) - aggregate given maps into a single map",
    extended = "Aggregate maps,returns as a HashMap")
public class UDAFUnionMap extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        return new Evaluator();
    }

    public static class State implements AggregationBuffer{
        HashMap<Object,Object> map = new HashMap<>();
    }

    public static class Evaluator extends GenericUDAFEvaluator{
        ObjectInspector inputOI;
        MapObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m,parameters);
            if (m==Mode.COMPLETE || m==Mode.PARTIAL1){
                inputOI = (MapObjectInspector)parameters[0];
            } else {
                internalMergeOI = (MapObjectInspector) parameters[0];
            }
            return ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new State();
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((State)aggregationBuffer).map.clear();
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            if (objects[0]!=null){
                State state = (State) aggregationBuffer;
                state.map.putAll((Map<?,?>)ObjectInspectorUtils.copyToStandardObject(objects[0],inputOI));
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return ((State)aggregationBuffer).map;
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            if (o!=null){
                State state = (State)aggregationBuffer;
                Map<?,?> pset = (Map<?,?>)ObjectInspectorUtils.copyToStandardObject(o,internalMergeOI);
                state.map.putAll(pset);
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            return ((State)aggregationBuffer).map;
        }
    }
}
