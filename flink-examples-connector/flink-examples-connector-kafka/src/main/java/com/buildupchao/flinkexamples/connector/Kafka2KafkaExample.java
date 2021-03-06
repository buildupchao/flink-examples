package com.buildupchao.flinkexamples.connector;

import com.buildupchao.flinkexamples.common.model.MetricEvent;
import com.buildupchao.flinkexamples.common.schema.MetricSchema;
import com.buildupchao.flinkexamples.common.utils.ExecutionEnvUtil;
import com.buildupchao.flinkexamples.common.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author ZHANGYACHAO
 * @date 2020/5/16 16:44
 * @since JDK1.8
 **/
public class Kafka2KafkaExample {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> dataStreamSource = KafkaConfigUtil.buildSource(env);

        dataStreamSource.addSink(new FlinkKafkaProducer011<>(
                    parameterTool.get("kafka.sink.brokers"),
                    parameterTool.get("kafka.sink.topic"),
                    new MetricSchema())
                ).name("flink-connectors-kafka")
                .setParallelism(parameterTool.getInt("stream.sink.parallelism"));

        env.execute("flink learning connectors kafka");
    }
}
