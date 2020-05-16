package com.buildupchao.flinkexamples.streaming;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author buildupchao
 * @date 2020/01/05 03:09
 * @since JDK 1.8
 */
public class IterateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        String topic = "people";
        DataStreamSource<String> dataStream = environment.addSource(new FlinkKafkaConsumer010<>(topic,
                new SimpleStringSchema(), properties));

        IterativeStream<People> iterativeStream = dataStream.map(new MapFunction<String, People>() {
            @Override
            public People map(String value) throws Exception {
                return JSON.parseObject(value, People.class);
            }
        }).iterate();

        SingleOutputStreamOperator<People> feedback = iterativeStream.filter(new FilterFunction<People>() {
            @Override
            public boolean filter(People value) throws Exception {
                return "buildupchao".equals(value.getName());
            }
        });

        // 如果有符合feedback过滤条件的数据，比如：name为buildupchao的，会持续不断的循环输出
        feedback.print("feedback:");

        iterativeStream.closeWith(feedback);

        SingleOutputStreamOperator<People> result = iterativeStream.filter(new FilterFunction<People>() {
            @Override
            public boolean filter(People value) throws Exception {
                return !"buildupchao".equals(value.getName());
            }
        });

        result.print("result:");


        // split
        SplitStream<People> split = iterativeStream.split(new OutputSelector<People>() {
            @Override
            public Iterable<String> select(People value) {
                ArrayList<String> list = new ArrayList<>();
                if ("male".equals(value.getSex())) {
                    list.add("male");
                } else {
                    list.add("female");
                }
                return list;
            }
        });

        DataStream<People> male = split.select("male");
        male.print("male:");

        iterativeStream.closeWith(male);

        environment.execute("IterateOperator");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class People {
        private String name;
        private Integer age;
        private String sex;
    }
}
