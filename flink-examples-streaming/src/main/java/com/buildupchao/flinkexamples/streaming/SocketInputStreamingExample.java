package com.buildupchao.flinkexamples.streaming;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author buildupchao
 * @date 2020/01/05 01:48
 * @since JDK 1.8
 */
public class SocketInputStreamingExample {

    /**
     * <p>
     *     启动程序前，先在terminal中执行<pre>nc -lk 9999 ```</pre>命令，不然程序无法启动。
     *     然后，即可在terminal向9999端口发送数据。
     * </p>
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 获取执行环境变量
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从Socket端口中接入数据
        DataStreamSource<String> socketDataStream = environment.socketTextStream("localhost", 9999);
        // 转换数据信息
        socketDataStream
                .map(data -> String.format("received data: %s", data))
                .print();

        // 提交执行Job
        environment.execute("SocketInputStreamingExample");
    }
}
