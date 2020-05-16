package com.buildupchao.flinkexamples.streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author buildupchao
 * @date 2020/01/05 04:10
 * @since JDK 1.8
 */
public class SocketOutputStreamingExample {

    /**
     * <p>
     *     启动程序前，先在terminal中执行<pre>nc -lk 9999 ```</pre>命令，不然程序无法启动。
     * </p>
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 获取执行环境变量
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 向Socket端口中写入数据
        environment.generateSequence(1, 20000)
                .setParallelism(2)
                .map(v -> String.format("%s\n", v))
                .writeToSocket("localhost",9999, new SimpleStringSchema());

        // 提交执行Job
        environment.execute("SocketOutputStreamingExample");
    }
}
