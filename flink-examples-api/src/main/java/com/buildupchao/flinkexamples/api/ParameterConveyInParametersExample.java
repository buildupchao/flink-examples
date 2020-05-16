package com.buildupchao.flinkexamples.api;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * @author buildupchao
 * @date 2020/01/02 00:05
 * @since JDK 1.8
 */
public class ParameterConveyInParametersExample {

    /**
     * 可以使用constructor or withParameters(Configuration) or global parameters方法将参数传递给函数。
     * 参数被序列化成函数对象的一部分，并传递到所有并行任务实例。
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Long> dataSet = environment.generateSequence(1, 10);

        Configuration parameters = new Configuration();
        parameters.setInteger("minThreshold", 7);

        // convey parameters in constructor
        dataSet.filter(new MyRichFilterFunction()).withParameters(parameters).print();
    }

    private static class MyRichFilterFunction extends RichFilterFunction<Long> {

        private long minThreshold;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.minThreshold = parameters.getInteger("minThreshold", 0);
        }

        @Override
        public boolean filter(Long value) throws Exception {
            return value > minThreshold;
        }
    }
}
