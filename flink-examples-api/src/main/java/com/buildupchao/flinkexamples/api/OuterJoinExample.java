package com.buildupchao.flinkexamples.api;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;

/**
 * outer join: left join, right join, full join
 *
 * @author buildupchao
 * @date 2020/01/01 15:39
 * @since JDK 1.8
 */
public class OuterJoinExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<Integer, String, Integer>> personList = Lists.newArrayList(
                Tuple3.of(101, "王朝", 0),
                Tuple3.of(102, "马汉", 1),
                Tuple3.of(104, "张龙", 0)
        );

        List<Tuple3<Integer, String, Integer>> learningCourseList = Lists.newArrayList(
                Tuple3.of(101, "Flink", 0),
                Tuple3.of(102, "Spark", 1),
                Tuple3.of(103, "Data-Warehouse", 0)
        );

        DataSource<Tuple3<Integer, String, Integer>> personDataSource = environment.fromCollection(personList);
        DataSource<Tuple3<Integer, String, Integer>> learningCourseDataSource = environment.fromCollection(learningCourseList);

        // left outer join, may second tuple be null.
        personDataSource.leftOuterJoin(learningCourseDataSource)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple3<Integer, String, Integer>, Tuple3<Integer, String, Integer>, Object>() {
                    @Override
                    public Object join(Tuple3<Integer, String, Integer> first, Tuple3<Integer, String, Integer> second) throws Exception {
                        if (second == null) {
                            return Tuple3.of(first.f0, first.f1, "null");
                        } else {
                            return Tuple3.of(first.f0, first.f1, second.f1);
                        }
                    }
                }).print();
        System.out.println("-------------------------------------------");


        // right outer join, may first tuple be null.
        personDataSource.rightOuterJoin(learningCourseDataSource)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple3<Integer, String, Integer>, Tuple3<Integer, String, Integer>, Object>() {
                    @Override
                    public Object join(Tuple3<Integer, String, Integer> first, Tuple3<Integer, String, Integer> second) throws Exception {
                        if (first == null) {
                            return Tuple3.of(second.f0, "null", second.f1);
                        } else {
                            return Tuple3.of(first.f0, first.f1, second.f1);
                        }
                    }
                }).print();
        System.out.println("-------------------------------------------");

        // full outer join, may first or second tuple be null.
        personDataSource.fullOuterJoin(learningCourseDataSource)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple3<Integer, String, Integer>, Tuple3<Integer, String, Integer>, Object>() {
                    @Override
                    public Object join(Tuple3<Integer, String, Integer> first, Tuple3<Integer, String, Integer> second) throws Exception {
                        if (first == null && second != null) {
                            return Tuple3.of(second.f0, "null", second.f1);
                        } else if (first != null && second == null) {
                            return Tuple3.of(first.f0, first.f1, "null");
                        } else if (first != null && second != null) {
                            return Tuple3.of(first.f0, first.f1, second.f1);
                        } else {
                            return Tuple3.of("null", "null", "null");
                        }
                    }
                }).print();
    }
}
