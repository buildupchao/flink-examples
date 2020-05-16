package com.buildupchao.flinkexamples.streaming;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author buildupchao
 * @date 2020/01/05 03:40
 * @since JDK 1.8
 */
public class RescalingPartitioningExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> rescaleDataStream = environment.generateSequence(1, 10).rescale();

        rescaleDataStream.print();

        environment.execute("RescalingPartitioningExample");
    }
}
