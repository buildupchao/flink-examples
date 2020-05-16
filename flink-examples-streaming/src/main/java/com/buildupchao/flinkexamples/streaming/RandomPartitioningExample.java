package com.buildupchao.flinkexamples.streaming;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author buildupchao
 * @date 2020/01/05 03:33
 * @since JDK 1.8
 */
public class RandomPartitioningExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> shuffleDataStream = environment.generateSequence(1, 25).shuffle();

        shuffleDataStream.print();

        environment.execute("RandomPartitioningExample");
    }
}
