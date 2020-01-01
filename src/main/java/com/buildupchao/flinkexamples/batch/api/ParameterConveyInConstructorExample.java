package com.buildupchao.flinkexamples.batch.api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author buildupchao
 * @date 2020/01/02 00:05
 * @since JDK 1.8
 */
public class ParameterConveyInConstructorExample {

    /**
     * 可以使用constructor or withParameters(Configuration) or global parameters方法将参数传递给函数。
     * 参数被序列化成函数对象的一部分，并传递到所有并行任务实例。
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Long> dataSet = environment.generateSequence(1, 10);

        // convey parameters in constructor
        dataSet.filter(new MyFilter(6L)).print();
    }

    private static class MyFilter implements FilterFunction<Long> {

        private long minThreshold;

        public MyFilter(long minThreshold) {
            this.minThreshold = minThreshold;
        }

        @Override
        public boolean filter(Long value) throws Exception {
            return value > minThreshold;
        }
    }
}
