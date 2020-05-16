package com.buildupchao.flinkexamples.dbus.incrementsync;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.buildupchao.flinkexamples.batch.dbus.function.DbusProcessFunction;
import com.buildupchao.flinkexamples.batch.dbus.model.Flow;
import com.buildupchao.flinkexamples.batch.dbus.schema.FlatMessageSchema;
import com.buildupchao.flinkexamples.batch.dbus.sink.HbaseSyncSink;
import com.buildupchao.flinkexamples.batch.dbus.source.FlowSource;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * 实时增量同步模块
 *
 * @author buildupchao
 * @date 2020/02/03 13:02
 * @since JDK 1.8
 */
public class IncrementSyncApp {

    public static final MapStateDescriptor<String, Flow> flowStateDescriptor = new MapStateDescriptor<String, Flow>(
            "flowBroadCastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<Flow>() {})
    );

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("group.id", "group1");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");
        properties.put("flink.partition-discovery.interval-millis", "30000");

        // 消费kafka数据
        FlinkKafkaConsumer010<FlatMessage> myConsumer = new FlinkKafkaConsumer010<FlatMessage>("test", new FlatMessageSchema(), properties);
        DataStream<FlatMessage> message = environment.addSource(myConsumer);

        // 同库，同表数据进入同一个分组，一个分区
        KeyedStream<FlatMessage, String> keyedMessage = message.keyBy(new KeySelector<FlatMessage, String>() {
            @Override
            public String getKey(FlatMessage value) throws Exception {
                return value.getDatabase() + value.getTable();
            }
        });

        // 读取配置流
        BroadcastStream<Flow> broadcast = environment.addSource(new FlowSource()).broadcast(flowStateDescriptor);

        DataStream<Tuple2<FlatMessage, Flow>> connectedStream = keyedMessage.connect(broadcast)
                .process(new DbusProcessFunction())
                .setParallelism(1);

        connectedStream.addSink(new HbaseSyncSink());

        environment.execute("IncrementSyncApp");
    }
}
