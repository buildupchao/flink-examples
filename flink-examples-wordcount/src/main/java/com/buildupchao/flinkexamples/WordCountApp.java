package com.buildupchao.flinkexamples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author buildupchao
 * @date 2020/05/16 10:44
 * @since JDK 1.8
 */
public class WordCountApp {

    private static final String[] WORDS = new String[] {
      "To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer"
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.fromElements(WORDS)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = value.toLowerCase().split("\\W+");
                        for (String word : words) {
                            collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        }).print();

        env.execute("word-count-based-on-flink");
    }
}
