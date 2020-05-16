package com.buildupchao.flinkexamples.sql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @author buildupchao
 * @date 2020/01/13 22:17
 * @since JDK 1.8
 */
public class BatchSqlExample {

    public static void main(String[] args) throws Exception {
        // 1.创建一个TableEnvironment
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(environment);
        // 2.读取数据源
        DataSet<String> dataSet = environment.readTextFile("data/tempUser.txt");
        DataSet<User> userDataSet = dataSet.map((value) -> {
            String[] segments = value.split(",");
            return User.builder()
                    .uid(segments[0])
                    .age(segments[1])
                    .sex(segments[2])
                    .career(segments[3])
                    .city(segments[4])
                    .build();
        });
        // 3.注册DataSet为表
        tableEnvironment.registerDataSet("user1", userDataSet);
        // 4.查询表数据
        Table careerTable = tableEnvironment.sqlQuery("select * from user1").select("career").distinct();
        // 5.将表转换为DataSet
        DataSet<String> resultDataSet = tableEnvironment.toDataSet(careerTable, String.class);
        resultDataSet.print();
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User {
        private String uid;
        private String age;
        private String sex;
        private String career;
        private String city;
    }
}
