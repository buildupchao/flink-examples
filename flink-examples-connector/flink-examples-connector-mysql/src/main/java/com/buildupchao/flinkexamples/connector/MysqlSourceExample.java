package com.buildupchao.flinkexamples.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author buildupchao
 * @date 2020/5/16 17:11
 * @since JDK1.8
 **/
public class MysqlSourceExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MysqlSource()).print();

        env.execute("Flink add data source");
    }
}
