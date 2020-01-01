package com.buildupchao.flinkexamples.batch.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author buildupchao
 * @date 2020/01/01 19:21
 * @since JDK 1.8
 */
public class BroadcastExample {

    /**
     * 跟DataStream的Broadcast State有些许类似。
     * 广播变量允许将数据集提供给的operator所有的并行实例，该数据集将作为集合在operator中进行访问。
     * <p>
     * 注意：由于广播变量的内容保存在每个节点的内容中，因此它不应该太大。
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 1.The DataSet to be broadcast
        DataSet<HashMap<Long, String>> broadcastDataSet =
                environment.fromElements(
                    Tuple2.of(101L, "王朝"), Tuple2.of(102L, "马汉"), Tuple2.of(103L, "张龙"), Tuple2.of(104L, "赵虎")
                )
                .map(new MapFunction<Tuple2<Long, String>, HashMap<Long, String>>() {
                    @Override
                    public HashMap<Long, String> map(Tuple2<Long, String> tuple2) throws Exception {
                        return new HashMap<Long, String>() {{
                            put(tuple2.f0, tuple2.f1);
                        }};
                    }
                });

        // 2.产生处理数据
        DataSet<String> dataSetResult = environment.generateSequence(100, 105)
                .map(new RichMapFunction<Long, String>() {

                    List<HashMap<Long, String>> broadcastData = new ArrayList<>();

                    Map<Long, String> allUserMap = Maps.newHashMap();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 不要去掉，因为外部可能会指定parameters配置参数
                        super.open(parameters);
                        // 3.获取广播变量
                        this.broadcastData = getRuntimeContext().getBroadcastVariable("userBroadcastData");
                        for (HashMap<Long, String> map : broadcastData) {
                            allUserMap.putAll(map);
                        }
                    }

                    @Override
                    public String map(Long userId) throws Exception {
                        String userName = allUserMap.getOrDefault(userId, "unknown");
                        return userName + " for userId#" + userId;
                    }

                    // 2.广播数据
                }).withBroadcastSet(broadcastDataSet, "userBroadcastData");

        dataSetResult.print();
    }
}
