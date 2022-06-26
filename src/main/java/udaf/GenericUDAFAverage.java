package udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;

public class GenericUDAFAverage extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(GenericUDAFAverage.class.getName());

    //读入参数类型校验，满足条件时返回聚合函数数据处理对象

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1){
            //            此处主要是对数据的长度做判断，因为是要计算均值，所以这个值的（一条一条读进来）
            throw new UDFArgumentTypeException(info.length - 1,"Exactly one argument is expected.");
        }
        if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            //            此处由于是要计算均值，所以规定数据类型智能是基础类型
            throw new UDFArgumentTypeException(0,"Only primitive type arguments are accepted but" +
                    info[0].getTypeName()+" is passed.");
        }

        switch (((PrimitiveTypeInfo) info[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case TIMESTAMP:
                return new GenericUDAFAverageEvaluator();
            case BOOLEAN:
            default:
                throw new UDFArgumentTypeException(0,"Only numeric or string type arguments are accepted but " +
                        info[0].getTypeName()+" is passed.");
        }


    }

    /**
     * GenericUDAFAverageEvaluator.
     * 自定义静态内部类：数据处理类，继承GenericUDAFEvaluator抽象类
     */
    public static class GenericUDAFAverageEvaluator extends GenericUDAFEvaluator{
        //1.1.定义全局输入输出数据的类型OI实例，用于解析输入输出数据
        // input For PARTIAL1 and COMPLETE
        //定义输入类型，数据的输入类型为PrimitiveObjectInspector
        PrimitiveObjectInspector inputOI;

        //定义中间结果的输入
        StructObjectInspector soi;
        StructField countField;
        StructField sumField;
        LongObjectInspector countFieldOI;
        DoubleObjectInspector sumFieldOI;

        //1.2.定义全局输出数据的类型，用于存储实际数据
        // output For PARTIAL1 and PARTIAL2
        Object[] partialResult;

        // output For FINAL and COMPLETE
        //计算完均值，输出
        DoubleWritable result;

        /*
         * 初始化：对各个模式处理过程，提取输入数据类型OI，返回输出数据类型OI
         * .每个模式（Mode）都会执行初始化
         * 1.输入参数parameters：
         * .1.1.对于PARTIAL1 和COMPLETE模式来说，是原始数据（单值）
         *    .设定了iterate()方法的输入参数的类型OI为：
         *    .		 PrimitiveObjectInspector 的实现类 WritableDoubleObjectInspector 的实例
         *    .		 通过输入OI实例解析输入参数值
         * .1.2.对于PARTIAL2 和FINAL模式来说，是模式聚合数据（双值）
         *    .设定了merge()方法的输入参数的类型OI为：
         *    .		 StructObjectInspector 的实现类 StandardStructObjectInspector 的实例
         *    .		 通过输入OI实例解析输入参数值
         * 2.返回值OI：
         * .2.1.对于PARTIAL1 和PARTIAL2模式来说，是设定了方法terminatePartial()返回值的OI实例
         *    .输出OI为 StructObjectInspector 的实现类 StandardStructObjectInspector 的实例
         * .2.2.对于FINAL 和COMPLETE模式来说，是设定了方法terminate()返回值的OI实例
         *    .输出OI为 PrimitiveObjectInspector 的实现类 WritableDoubleObjectInspector 的实例
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m,parameters);

            // init input
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE){
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                //部分数据作为输入参数时，用到的struct的OI实例，指定输入数据类型，用于解析数据
                soi = (StructObjectInspector) parameters[0];
                countField = soi.getStructFieldRef("count");
                sumField = soi.getStructFieldRef("sum");
                //数组中的每个数据，需要其各自的基本类型OI实例解析
                countFieldOI = (LongObjectInspector) countField.getFieldObjectInspector();
                sumFieldOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
            }

            // init output
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2){
                //部分聚合结果是一个数组，要给里面放入结果
                partialResult = new Object[2];
                partialResult[0] = new LongWritable(0);
                partialResult[1] = new DoubleWritable(0);

                /*
                 * .构造Struct的OI实例，用于设定聚合结果数组的类型
                 * .需要字段名List和字段类型List作为参数来构造
                 */
                ArrayList<String> fname = new ArrayList<>();
                fname.add("count");
                fname.add("sum");

                ArrayList<ObjectInspector> foi = new ArrayList<>();
                //注：此处的两个OI类型 描述的是 partialResult[] 的两个类型，故需一致
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname,foi);
            } else {
                //FINAL 最终聚合结果为一个数值，并用基本类型OI设定其类型
                result = new DoubleWritable(0);
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }
        }

        /*
         * .聚合数据缓存存储结构
         */
        static class AverageAgg implements AggregationBuffer{
            long count;
            double sum;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AverageAgg result = new AverageAgg();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            AverageAgg myagg = (AverageAgg) aggregationBuffer;
            myagg.count = 0;
            myagg.sum = 0;
        }

        boolean warned = false;

        //遍历原始数据
        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            assert (objects.length == 1);
            Object p = objects[0];
            if (p!=null){
                AverageAgg myagg = (AverageAgg) aggregationBuffer;
                try {
                    //通过基本数据类型OI解析Object p的值
                    double v = PrimitiveObjectInspectorUtils.getDouble(p,inputOI);
                    //myagg.count更新为 21,myagg.sum 更新为2000+100=2100
                    myagg.count++;
                    myagg.sum+=v;
                } catch (NumberFormatException e){
                    if (!warned){
                        warned = true;
                        LOG.warn(getClass().getSimpleName()+" "+ StringUtils.stringifyException(e));
                        LOG.warn(getClass().getSimpleName()+" ignoring similar exceptions.");
                    }
                }
            }
        }

        /*
         * .得出部分聚合结果
         */
        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            //myagg.count更新为 21,myagg.sum 2100
            AverageAgg myagg = (AverageAgg) aggregationBuffer;
            //LongWritable 强制转化为long  writable
            ((LongWritable) partialResult[0]).set(myagg.count);
            ((DoubleWritable) partialResult[1]).set(myagg.sum);
//            partialResult相当于一个两个元素的object列表。
            return partialResult;
        }

        /*
         * .合并部分聚合结果
         * .注：Object[] 是 Object 的子类，此处 partial 为 Object[]数组
         */
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            if (o != null){
                //myagg.count更新为 21,myagg.sum 2100
                AverageAgg myagg = (AverageAgg) aggregationBuffer;
                //通过StandardStructObjectInspector实例，分解出 partial 数组元素值
                Object partialCount = soi.getStructFieldData(o,countField);
                Object partialSum = soi.getStructFieldData(o,sumField);
                //通过基本数据类型的OI实例解析Object的值
                myagg.count += countFieldOI.get(partialCount);
                myagg.sum += sumFieldOI.get(partialSum);
            }
        }

        /*
         * .得出最终聚合结果
         */
        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            AverageAgg myagg = (AverageAgg) aggregationBuffer;
            if (myagg.count == 0){
                return null;
            } else {
                result.set(myagg.sum/myagg.count);
                return result;
            }
        }
    }
}
