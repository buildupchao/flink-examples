package com.buildupchao.flinkexamples.streaming;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author buildupchao
 * @date 2020/01/06 19:41
 * @since JDK 1.8
 */
public class AssingerUsedForEventTimeAndWatermarkExample {

    /**
     * 在Flink系统找那个实现了两种Periodic Watermark Assigner:
     * 一种为升序模式，会将数据中的Timestamp根据
     * 指定字段提取，并用当前的Timestamp作为最新的Watermark，这种Timestamp Assigner比较适合事件按照顺序生
     * 成，没有乱序事件的情况；
     * 另外一种是通过设定固定的时间间隔来指定Watermark落后于Timestamp的区间长度，也
     * 就是最长容忍迟到多长时间内的数据到达系统。
     *
     * @param args
     */
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 指定系统时间概念为EventTime
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<String, Long, Integer>> dataStream =
                environment.fromCollection(Lists.newArrayList(
                        Tuple3.of("a", 1L, 1),
                        Tuple3.of("b", 1L, 1),
                        Tuple3.of("b", 3L, 1)
                ));
        // 使用系统默认Ascending分配事件信息和Watermark
        DataStream<Tuple3<String, Long, Integer>> withTimestampAndWatermarks =
                dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<String, Long, Integer> element) {
                        return element.f2.longValue();
                    }
                });

        /*DataStream<Tuple3<String, Long, Integer>> withTimestampAndWatermarks =
                dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Integer>>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element) {
                        return element.f1;
                    }
                });*/

        // 对数据集进行窗口运算
        DataStream<Tuple3<String, Long, Integer>> result =
                withTimestampAndWatermarks.keyBy(0).timeWindow(Time.seconds(10)).sum(1);
        result.print();
    }
}
