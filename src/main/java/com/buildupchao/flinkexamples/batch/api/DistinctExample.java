package com.buildupchao.flinkexamples.batch.api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 去重
 *
 * @author buildupchao
 * @date 2020/01/01 15:04
 * @since JDK 1.8
 */
public class DistinctExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Tuple3<Long, String, Integer>> dataSource = environment.fromElements(
                Tuple3.of(1L, "buildupchao", 25),
                Tuple3.of(2L, "xiaoxiaomo", 27),
                Tuple3.of(3L, "wuning", 28),
                Tuple3.of(3L, "fan6", 28),
                Tuple3.of(3L, "wangming", 29),
                Tuple3.of(4L, "fanyi", 28)
        );

        dataSource.distinct(0).print();
    }
}
