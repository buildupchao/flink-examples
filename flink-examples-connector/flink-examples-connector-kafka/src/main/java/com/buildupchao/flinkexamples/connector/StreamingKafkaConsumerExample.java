package com.buildupchao.flinkexamples.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.Properties;

/**
 * @author buildupchao
 * @date 2020/01/02 01:31
 * @since JDK 1.8
 */
public class StreamingKafkaConsumerExample {

    /**
     * 批处理Connector之文件系统访问:
     * <p>
     * Flink内置了对HDFS、S3、MapR、Alluxio文件系统的支持<br/>
     * 注意：FLink允许用户使用实现{#link org.apache.hadoop.fs.FileSystem}接口的任何文件系统。
     * <strong>
     * S3、Google Cloud Storage Connector for Hadoop、Alluxio、XtreemFS、FTP via Hftp and many more
     * </strong>
     * <hr/>
     * Hadoop兼容性：使用Hadoop的Input/OutputFormat wrappers连接到其他系统
     * <p>
     * Flink与Apache Hadoop MapReduce接口兼容，因此允许重用Hadoop MapReduce实现的代码：
     * 使用Hadoop Writable data type
     * 使用任何Hadoop InputFormat作为DataSource（flink内置HadoopInputFormat）
     * 使用任何Hadoop OutputFormat作为DataSink（flink内置HadoopOutputFormat）
     * 使用Hadoop Mapper作为FlatMapFunction
     * 使用Hadoop Reducer作为GroupReduceFunction
     * 引入依赖：
     * <dependency>
     * <groupId>org.apache.flink</groupId>
     * <artifactId>flink-hadoop-compatibility_2.11</artifactId>
     * <version>1.9.1</version>
     * </dependency>
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "flink-kafka-streaming";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink2kafka");

        FlinkKafkaConsumer09<String> kafkaConsumer = new FlinkKafkaConsumer09<>(topic, new SimpleStringSchema(), properties);

        DataStream<String> dataStream = environment.addSource(kafkaConsumer);
        dataStream.print();

        environment.execute("StreamingKakfaProducer");
    }
}
