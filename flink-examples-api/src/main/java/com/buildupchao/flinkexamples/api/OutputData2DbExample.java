package com.buildupchao.flinkexamples.api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.types.Row;

import java.sql.Types;

/**
 * @author buildupchao
 * @date 2020/01/01 16:32
 * @since JDK 1.8
 */
public class OutputData2DbExample {

    /**
     * writeAsText/TextOutputFormat
     * writeAsFormattedText/TextOutputFormat
     * writeAsCsv/CsvOutputFormat
     * print/printToErr/print(String msg)/printToError(String msg): 线上应用杜绝使用，采用抽样打印或者日志的方式
     * write/FileOutputFormat
     * output/OutputFormat: 通用的输出方法，用于不基于文件的数据接收器（如将数据存储在数据库中）
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.readCsvFile("data/user.csv")
                .includeFields("11100")
                .ignoreFirstLine()
                .ignoreComments("##")
                .ignoreInvalidLines()
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .types(Integer.class, Integer.class, String.class)
                .map(tuple3 -> {
                    Row row = new Row(3);
                    row.setField(0, tuple3.f0);
                    row.setField(1, tuple3.f1);
                    row.setField(2, tuple3.f2);
                    return row;
                }).output(
                    JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setDBUrl("jdbc:mysql://localhost:3306/flink-examples")
                        .setUsername("root")
                        .setPassword("root")
                        .setQuery("insert into user(id, age, gender) values(?, ?, ?)")
                        .setSqlTypes(new int[]{Types.INTEGER, Types.INTEGER, Types.VARCHAR})
                        .setBatchInterval(1)
                        .finish()
                ).getDataSet().print();

    }
}
