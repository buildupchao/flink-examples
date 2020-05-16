package com.buildupchao.flinkexamples.streaming;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author buildupchao
 * @date 2020/01/05 03:00
 * @since JDK 1.8
 */
public class ProjectExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple4> dataStream = environment.fromElements(TRANSCRIPT);

        // 获取姓名、成绩
        SingleOutputStreamOperator<Tuple> projectStream = dataStream.project(1, 3);

        projectStream.print();

        environment.execute("ProjectExample");
    }

    public static final Tuple4[] TRANSCRIPT = new Tuple4[]{
            Tuple4.of("class1", "张三", "语文", 100),
            Tuple4.of("class1", "李四", "语文", 78),
            Tuple4.of("class1", "王五", "语文", 99),
            Tuple4.of("class2", "赵六", "语文", 81),
            Tuple4.of("class2", "钱七", "语文", 59),
            Tuple4.of("class2", "马二", "语文", 97)
    };
}
