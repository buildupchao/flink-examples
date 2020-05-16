package com.buildupchao.flinkexamples.api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * 递归读取文件数据
 *
 * @author buildupchao
 * @date 2020/01/01 14:09
 * @since JDK 1.8
 */
public class RecursionReadExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        Configuration parameters = new Configuration();

        // set the recursive enumeration parameter
        parameters.setBoolean("recursive.file.enumeration", true);

        String filepath = System.getProperty("user.dir") + "/data/files";
        DataSource<String> logs = environment.readTextFile("file:///" + filepath)
                .withParameters(parameters);

        logs.print();
    }
}
