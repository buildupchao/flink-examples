package com.buildupchao.flinkexamples.batch.api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

/**
 * @author buildupchao
 * @date 2020/01/01 16:03
 * @since JDK 1.8
 */
public class OutputData2TextExample {

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

        String outputPath = System.getProperty("user.dir") + "/data/user.txt";
        environment.readCsvFile("data/user.csv")
                .includeFields("11100")
                .ignoreFirstLine()
                .ignoreComments("##")
                .ignoreInvalidLines()
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .types(Integer.class, Integer.class, String.class)
                .writeAsFormattedText("file:///" + outputPath, FileSystem.WriteMode.OVERWRITE, new MyTextFormatter())
                .setParallelism(1)
                .getDataSet()
                .print();
    }

    private static class MyTextFormatter implements TextOutputFormat.TextFormatter<Tuple3<Integer, Integer, String>> {

        @Override
        public String format(Tuple3<Integer, Integer, String> value) {
            return String.format("userId(%d)'s age is %d, %s.", value.f0, value.f1, value.f2);
        }
    }
}
