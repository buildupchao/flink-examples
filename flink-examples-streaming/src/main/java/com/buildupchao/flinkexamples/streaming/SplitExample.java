package com.buildupchao.flinkexamples.streaming;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @author buildupchao
 * @date 2020/01/05 02:41
 * @since JDK 1.8
 */
public class SplitExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> dataStream = environment.fromElements(
                new Tuple2<>("a", 3),
                new Tuple2<>("d", 4),
                new Tuple2<>("c", 2),
                new Tuple2<>("c", 5),
                new Tuple2<>("a", 5)
        );

        SplitStream<Tuple2<String, Integer>> splitStream = dataStream.split((OutputSelector<Tuple2<String, Integer>>) value -> {
            List<String> output = Lists.newArrayList();
            if (value.f1 % 2 == 0) {
                output.add("even");
            } else {
                output.add("odd");
            }
            return output;
        });

//        splitStream.select("even").print();

        splitStream.select("odd").print();

//        splitStream.select("even, odd").print();

        environment.execute("SplitExample");
    }
}
