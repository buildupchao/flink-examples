package com.buildupchao.flinkexamples.stream;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;

/**
 * @author buildupchao
 * @date 2020/01/05 03:43
 * @since JDK 1.8
 */
public class CustomPartitioningExample {

    private static Tuple2<String, Integer>[] ELEMENTS = new Tuple2[]{
            Tuple2.of("flink", 1),
            Tuple2.of("storm", 2),
            Tuple2.of("spark", 3),
            Tuple2.of("flink2", 4),
            Tuple2.of("learn flink", 5),
            Tuple2.of("deep in flink", 6),
            Tuple2.of("Thinking in flink", 7)
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> partitionDataStream = environment
                .fromElements(ELEMENTS)
                .rebalance()
                .partitionCustom(new MyPartitioner(), 0);

        partitionDataStream.print();

        environment.execute("CustomPartitioningExample");
    }

    public static class MyPartitioner implements Partitioner<String> {

        private Random random = new Random();

        @Override
        public int partition(String key, int numPartitions) {
            if (key.contains("flink")) {
                return 0;
            } else {
                return random.nextInt(numPartitions);
            }
        }
    }
}
