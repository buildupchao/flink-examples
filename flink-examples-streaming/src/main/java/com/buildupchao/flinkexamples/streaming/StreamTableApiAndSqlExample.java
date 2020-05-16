package com.buildupchao.flinkexamples.streaming;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author buildupchao
 * @date 2020/01/13 21:40
 * @since JDK 1.8
 */
public class StreamTableApiAndSqlExample {

    public static void main(String[] args) {
        // 1.创建一个TableEnvironment
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);
        // 2.读取数据源
        DataStream<String> dataStream = streamExecutionEnvironment.readTextFile("data/tempUser.txt");
        DataStream<User> userDataStream = dataStream.map(new MapFunction<String, User>() {
            @Override
            public User map(String value) throws Exception {
                String[] segments = value.split(",");
                return User.builder()
                        .uid(segments[0])
                        .age(segments[1])
                        .sex(segments[2])
                        .career(segments[3])
                        .city(segments[4])
                        .build();
            }
        });
        // 3.将DataStream转换成Table
        Table table = tableEnvironment.fromDataStream(userDataStream);
        // 4.注册表
        tableEnvironment.registerTable("user1", table);
        // 5.获取表中所有信息
        Table resultTable = tableEnvironment.sqlQuery("select * from user1").select("uid");
        // 6.将表转化成DataStream
        DataStream<String> resultDataStream = tableEnvironment.toAppendStream(resultTable, String.class);
        resultDataStream.print();
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
