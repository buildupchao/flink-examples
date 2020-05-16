package com.buildupchao.flinkexamples.api;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author buildupchao
 * @date 2020/01/01 17:31
 * @since JDK 1.8
 */
public class OutputData2DbSinkExample {

    /**
     * 数据类型，按顺序，与数据库字段保持一致
     */
    private static final TypeInformation[] FIELD_TYPES = new TypeInformation[]{
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.DOUBLE_TYPE_INFO,
            BasicTypeInfo.LONG_TYPE_INFO
    };

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
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设定检查点
        environment.enableCheckpointing(5000);
        // 设定eventTime
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Kafka消费者参数
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
        consumerProperties.setProperty("group.id", "kafka2mysql");
        consumerProperties.setProperty("enable.auto.commit", "true");

        FlinkKafkaConsumer09<String> kafka2mysqlConsumer = new FlinkKafkaConsumer09<>(
                "kafka2mysql",
                new SimpleStringSchema(),
                consumerProperties
        );
        // 接收kafka中的参数
        DataStreamSource<String> kafkaDataStream = environment.addSource(kafka2mysqlConsumer);

        // 将json字符串解析成Row对象
        DataStream<Row> userLatestDataStream = kafkaDataStream.map(x -> JSON.parseObject(x))
                .map(x -> Row.of(
                        x.getInteger("id"),
                        x.getInteger("age"),
                        x.getString("gender"),
                        System.currentTimeMillis())
                );

        String updateSql = "INSERT INTO kafka_user(id, age, gender, update_time) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE age=VALUES(age), gender=VALUES(gender), update_time=VALUES(update_time)";

        // 写入数据到mysql
        JDBCAppendTableSink mysqlSink = JDBCAppendTableSink.builder()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/flink-examples")
                .setUsername("root")
                .setPassword("root")
                .setParameterTypes(FIELD_TYPES)
                .build();

        mysqlSink.emitDataStream(userLatestDataStream);

        // 执行
        String jobName = "user-info-job";
        environment.execute(jobName);
    }
}
