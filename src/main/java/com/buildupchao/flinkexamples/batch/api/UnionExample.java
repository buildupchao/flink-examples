package com.buildupchao.flinkexamples.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;

/**
 * @author buildupchao
 * @date 2020/01/01 15:10
 * @since JDK 1.8
 */
public class UnionExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<Integer, String>> personList = Lists.newArrayList(
            Tuple2.of(101, "王朝"),
            Tuple2.of(102, "马汉"),
            Tuple2.of(103, "张龙"),
            Tuple2.of(104, "赵虎")
        );

        List<Tuple2<Integer, String>> learningClassList = Lists.newArrayList(
          Tuple2.of(101, "Flink"),
          Tuple2.of(102, "Spark"),
          Tuple2.of(103, "Storm"),
          Tuple2.of(104, "Data-Warehouse")
        );

        DataSource<Tuple2<Integer, String>> personDataSource = environment.fromCollection(personList);
        DataSource<Tuple2<Integer, String>> learningClassDataSource = environment.fromCollection(learningClassList);

        DataSet<Tuple2<Integer, String>> resultData = personDataSource.union(learningClassDataSource);
        resultData.print();
    }
}
