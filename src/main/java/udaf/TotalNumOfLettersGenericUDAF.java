package udaf;

import com.sun.tools.corba.se.idl.InterfaceGen;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class TotalNumOfLettersGenericUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        //如果参数长度不等于，说明参数错误
        if (info.length != 1){
            throw new UDFArgumentTypeException(info.length - 1,"Exactly one argument is expected.");
        }
//        拿到参数
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info[0]);
//        /如果oi的数据不是八大基本类型，报错
        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,"Argument must be PRIMITIVE, but " +
                    oi.getCategory().name()+" was passed.");
        }

        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;
        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentTypeException(0,"Argument must be String,but " +
                    inputOI.getPrimitiveCategory().name()+" was passed.");
        }
        return new TotalNumOfLettersEvaluator();
    }

    public static class TotalNumOfLettersEvaluator extends GenericUDAFEvaluator{
        PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;
        PrimitiveObjectInspector integerOI;
        int total = 0;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m,parameters);

            //map阶段读取sql列，输入为String基础数据格式
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE){
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                //其余阶段，输入为Integer基础数据格式
                integerOI = (PrimitiveObjectInspector) parameters[0];
            }

            // 指定各个阶段输出数据格式都为Integer类型
            outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            return outputOI;
        }

//        存储当前字符总数的类
        static class LetterSumAgg implements AggregationBuffer{
            int sum = 0;
            void add(int num){
                sum += num;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            LetterSumAgg result = new LetterSumAgg();
            return result;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            LetterSumAgg myagg = new LetterSumAgg();
        }

        private boolean warned = false;

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            assert (objects.length == 1);
            if (objects[0]!=null){
                LetterSumAgg myagg = (LetterSumAgg) aggregationBuffer;
                Object p1 = ((PrimitiveObjectInspector)inputOI).getPrimitiveJavaObject(objects[0]);
                myagg.add(String.valueOf(p1).length());
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            LetterSumAgg myaggg = (LetterSumAgg) aggregationBuffer;
            total += myaggg.sum;
            return total;
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            if (o!=null){
                LetterSumAgg myagg1 = (LetterSumAgg) aggregationBuffer;
                Integer partialSum = (Integer) integerOI.getPrimitiveJavaObject(o);
                LetterSumAgg myagg2 = new LetterSumAgg();
                myagg2.add(partialSum);
                myagg1.add(myagg2.sum);
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            LetterSumAgg myagg = (LetterSumAgg) aggregationBuffer;
            total = myagg.sum;
            return myagg.sum;
        }
    }

}
