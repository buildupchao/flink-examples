package com.buildupchao.flinkexamples.batch.api;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

/**
 * MapPartition: 类似于map，一次处理一个分区的数据（如果在进行map处理的时候需要获取第三方资源，建议使用MapPartition）<br/>
 * 使用场景: 过滤脏数据、数据清洗等
 *
 * @author buildupchao
 * @date 2020/01/01 14:40
 * @since JDK 1.8
 */
public class MapPartitionExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Long> dataSource = environment.generateSequence(1, 20);

        dataSource.mapPartition(new MyMapPartitionFunction()).setParallelism(4).print();
    }

    private static class MyMapPartitionFunction implements MapPartitionFunction<Long, Long> {

        @Override
        public void mapPartition(Iterable<Long> values, Collector<Long> collector) throws Exception {
            long count = 0;
            for (Long value : values) {
                count++;
            }
            collector.collect(count);
        }
    }
}
