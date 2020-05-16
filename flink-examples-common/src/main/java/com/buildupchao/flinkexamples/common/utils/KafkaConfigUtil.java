package com.buildupchao.flinkexamples.common.utils;

import com.buildupchao.flinkexamples.common.constant.PropertiesConstants;
import com.buildupchao.flinkexamples.common.model.MetricEvent;
import com.buildupchao.flinkexamples.common.schema.MetricSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author buildupchao
 * @date 2020/5/16 15:29
 * @since JDK1.8
 **/
public class KafkaConfigUtil {

    public static Properties buildKafkaProps() {
        return buildKafkaProps(ParameterTool.fromSystemProperties());
    }

    public static Properties buildKafkaProps(ParameterTool parameterTool) {
        Properties props = parameterTool.getProperties();
        props.put("bootstrap.servers", parameterTool.get(PropertiesConstants.KAFKA_BROKERS, PropertiesConstants.DEFAULT_KAFKA_BROKERS));
        props.put("zookeeper.connect", parameterTool.get(PropertiesConstants.KAFKA_ZOOKEEPER_CONNECT, PropertiesConstants.DEFAULT_KAFKA_ZOOKEEPER_CONNECT));
        props.put("group.id", parameterTool.get(PropertiesConstants.KAFKA_GROUP_ID, PropertiesConstants.DEFAULT_KAFKA_GROUP_ID));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }

    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env) {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        String topic = parameterTool.getRequired(PropertiesConstants.METRICS_TOPIC);
        long time = parameterTool.getLong(PropertiesConstants.CONSUMER_FROM_TIME, 0L);
        return buildSource(env, topic, time);
    }

    private static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env, String topic, long time) {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties properties = buildKafkaProps(parameterTool);
        FlinkKafkaConsumer011<MetricEvent> consumer = new FlinkKafkaConsumer011<>(topic, new MetricSchema(), properties);
        if (time != 0L) {
            Map<KafkaTopicPartition, Long> partitionOffset = buildOffsetByTime(properties, parameterTool, time);
            consumer.setStartFromSpecificOffsets(partitionOffset);
        }
        return env.addSource(consumer);
    }

    private static Map<KafkaTopicPartition, Long> buildOffsetByTime(Properties properties, ParameterTool parameterTool, Long time) {
        properties.setProperty("group.id", "query_time_" + time);
        KafkaConsumer consumer = new KafkaConsumer(properties);
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(parameterTool.getRequired(PropertiesConstants.METRICS_TOPIC));
        Map<TopicPartition, Long> partitionInfoLongMap = partitionInfos.stream()
                .collect(HashMap::new, (map, partitionInfo) -> map.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), time), HashMap::putAll);
        Map<TopicPartition, OffsetAndTimestamp> offsetResult = consumer.offsetsForTimes(partitionInfoLongMap);
        Map<KafkaTopicPartition, Long> partitionOffset = offsetResult.entrySet().stream()
                .map(entry -> new Tuple2<>(new KafkaTopicPartition(entry.getKey().topic(), entry.getKey().partition()), entry.getValue().offset()))
                .collect(Collectors.toMap(v -> v.getField(0), v -> v.getField(1)));
        consumer.close();
        return partitionOffset;
    }
}
