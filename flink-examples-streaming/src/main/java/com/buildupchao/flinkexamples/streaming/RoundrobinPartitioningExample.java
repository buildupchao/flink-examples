package com.buildupchao.flinkexamples.streaming;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author buildupchao
 * @date 2020/01/05 03:36
 * @since JDK 1.8
 */
public class RoundrobinPartitioningExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 通过循环的方式对数据集中的数据进行重分区，能够尽可能保证每个分区的数据平衡，当数据集发生数据倾斜的时候
         * 使用这种策略就是比较有效的优化方法。
         */
        DataStream<Long> rebalanceDataStream = environment.generateSequence(1, 10).rebalance();

        rebalanceDataStream.print();

        environment.execute("RoundrobinPartitioningExample");
    }
}
