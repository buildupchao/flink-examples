package com.buildupchao.flinkexamples.stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author buildupchao
 * @date 2020/01/05 03:42
 * @since JDK 1.8
 */
public class BroadcastingExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> broadcastDataStream = environment.generateSequence(1, 12).broadcast();

        broadcastDataStream.print();

        environment.execute("BroadcastingExample");
    }
}
