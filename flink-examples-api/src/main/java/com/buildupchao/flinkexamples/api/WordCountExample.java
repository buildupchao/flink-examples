package com.buildupchao.flinkexamples.api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 词频统计
 *
 * @author buildupchao
 * @date 2020/01/01 12:38
 * @since JDK 1.8
 */
public class WordCountExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = environment.fromElements(
                "flink flink flink",
                "blink blink blink",
                "buildupchao is so handsome"
        );
        DataSet<Tuple2<String, Integer>> wordCountDataSource = dataSource.flatMap(new WordSplitter())
                .groupBy(0)
                .sum(1);

        wordCountDataSource.print();
    }

    private static class WordSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : line.split("\\s+")) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
