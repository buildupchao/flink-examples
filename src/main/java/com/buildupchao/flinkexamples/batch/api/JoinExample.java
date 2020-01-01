package com.buildupchao.flinkexamples.batch.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;

/**
 * inner join (equals join)
 *
 * @author buildupchao
 * @date 2020/01/01 15:17
 * @since JDK 1.8
 */
public class JoinExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<Integer, String, Integer>> personList = Lists.newArrayList(
                Tuple3.of(101, "王朝", 0),
                Tuple3.of(102, "马汉", 1),
                Tuple3.of(103, "张龙", 0),
                Tuple3.of(104, "赵虎", 0)
        );

        List<Tuple3<Integer, String, Integer>> learningCourseList = Lists.newArrayList(
                Tuple3.of(101, "Flink", 0),
                Tuple3.of(102, "Spark", 1),
                Tuple3.of(104, "Data-Warehouse", 0)
        );

        DataSource<Tuple3<Integer, String, Integer>> personDataSource = environment.fromCollection(personList);
        DataSource<Tuple3<Integer, String, Integer>> learningCourseDataSource = environment.fromCollection(learningCourseList);

        DataSet<UserInfo> userInfoDataSet =
                personDataSource.join(learningCourseDataSource)
                .where(0, 2)
                .equalTo(0, 2)
                .with(new UserInfoJonFunction());

        userInfoDataSet.print();
    }

    private static class UserInfoJonFunction implements JoinFunction<Tuple3<Integer, String, Integer>, Tuple3<Integer, String, Integer>, UserInfo> {

        @Override
        public UserInfo join(Tuple3<Integer, String, Integer> person,
                             Tuple3<Integer, String, Integer> learningCourse) throws Exception {
            return new UserInfo(person.f0, person.f1, learningCourse.f1);
        }
    }

    @Data
    @ToString
    @AllArgsConstructor
    private static class UserInfo {
        private Integer userId;
        private String userName;
        private String learningCourse;
    }
}
