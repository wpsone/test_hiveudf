package udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.Stat;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(name = "chooseone",value = "_FUNC_(value) - Return an arbitrary value from the group")
public class UDAFChooseOne extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length!=1){
            throw new UDFArgumentLengthException("Only one parameter expected,but you provided "+info.length);
        }
        return new Evaluator();
    }

    public static class Evaluator extends GenericUDAFEvaluator {
        ObjectInspector inputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m,parameters);
            inputOI = parameters[0];
            return ObjectInspectorUtils.getStandardObjectInspector(inputOI);
        }

        static class State implements AggregationBuffer {
            Object state = null;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new State();
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            State s = (State) aggregationBuffer;
            s.state = null;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            State s = (State) aggregationBuffer;
            if (s.state == null){
                s.state = ObjectInspectorUtils.copyToStandardJavaObject(objects[0],inputOI);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            State s = (State) aggregationBuffer;
            return s.state;
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            State s = (State) aggregationBuffer;
            if (s.state == null){
                s.state = ObjectInspectorUtils.copyToStandardObject(o,inputOI);
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            State s = (State) aggregationBuffer;
            return s.state;
        }
    }
}
