package com.buildupchao.flinkexamples.api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 读取csv文件数据
 *
 * @author buildupchao
 * @date 2020/01/01 13:41
 * @since JDK 1.8
 */
public class ReadCsvExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 用户id,年龄,性别,职业,城市
        DataSource<Tuple3<Integer, Integer, String>> csvDataSource = environment.readCsvFile("data/user.csv")
                .includeFields("11100")
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .ignoreComments("##")
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .types(Integer.class, Integer.class, String.class);

        csvDataSource.print();
    }
}
