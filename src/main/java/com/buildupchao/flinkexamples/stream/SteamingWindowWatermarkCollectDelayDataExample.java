package com.buildupchao.flinkexamples.stream;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * 代码解读：
 * #1，接收socket数据
 * #2，将每行数据按照逗号分隔，转换为Tuple2<String, Long>类型。第一个元素：具体的数据；第二个元素：数据的eventTime
 * #3，抽取timestamp，生成watermark，允许的最大乱序时间是10s，并打印（threadId, key, eventTime, currentMaxTimestamp, watermark）等
 * #4，分组聚合，window窗口大小为3秒，输出（key, 窗口内元素个数，窗口中最早元素的时间，窗口中最晚元素的时间，窗口自身开始时间，窗口自身结束时间）
 * #5，收集迟到的数据并打印（实际生产中可以输出到其他存储介质中），此处涉及到<pre>SingleOutputStreamOperator</pre>
 * <p>
 * 对于乱序的数据，Flink可以通过watermark机制结合window的操作，来处理一定范围内的乱序数据。
 *
 * 对于"迟到（late element）"太多的数据，Flink怎么处理？
 * <ol>
 *     <li>1.丢弃（默认）</li>
 *     <li>2.</li>
 * </ol>
 *
 * @author buildupchao
 * @date 2020/01/16 16:06
 * @since JDK 1.8
 */
public class SteamingWindowWatermarkCollectDelayDataExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(SteamingWindowWatermarkCollectDelayDataExample.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置使用EventTime（默认我ProcessTime）
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置并行度为1，默认并行度是当前机器的CPU数量
        env.setParallelism(1);

        int port = 9999;
        // 连接socket获取输入的数据
        DataStream<String> textStream = env.socketTextStream("localhost", port, "\n");

        // 解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = textStream.map(new MapFunction<String, Tuple2<String,
                Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] segments = value.split(",");
                return Tuple2.of(segments[0], Long.parseLong(segments[1]));
            }
        });

        // 抽取timestamp和生成watermark
        DataStream<Tuple2<String, Long>> watermarkStream = inputMap
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

                    Long currentMaxTimestamp = 0L;
                    // 最大允许的乱序时间为10s
                    final Long maxOutOfOrderness = 10000L;

                    FastDateFormat timeFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

                    /**
                     * 定义生成watermark的逻辑
                     * 默认100ms被调用一次
                     * @return
                     */
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }

                    /**
                     * 定义如何提取timestamp
                     * @param element
                     * @param previousElementTimestamp
                     * @return
                     */
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                        long timestamp = element.f1;
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        long id = Thread.currentThread().getId();

                        LOGGER.info(
                                "currentThreadId:{}, key:{}, eventTime:[{}|{}], currentMaxTimestamp:[{}|{}], " +
                                        "watermark:[{}|{}]",
                                id,
                                element.f0,
                                element.f1, timeFormat.format(element.f1),
                                currentMaxTimestamp, timeFormat.format(currentMaxTimestamp),
                                getCurrentWatermark().getTimestamp(),
                                timeFormat.format(getCurrentWatermark().getTimestamp())
                        );
                        return timestamp;
                    }
                });

        // 保存被丢弃的数据
        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<>("late-data");

        // 由于getSideOutput方法时SingleOutputStreamOperator子类中的特有方法，所以不采用父类DataStream
        // 按照消息的EventTime分配窗口，和调用TimeWindow效果一样
        // allowedLateness(Time.seconds(2)) 指允许数据迟到两秒
        SingleOutputStreamOperator<String> window = watermarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                //.allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    /**
                     * 对window内的数据进行排序，保证数据的顺序
                     * @param tuple
                     * @param window
                     * @param input
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input,
                                      Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        List<Long> timestampList = Lists.newArrayList();
                        input.iterator().forEachRemaining(v -> {
                            timestampList.add(v.f1);
                        });

                        Collections.sort(timestampList);
                        FastDateFormat timeFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = String.format(
                                "%s,%d,%s,%s,%s,%s",
                                key,
                                timestampList.size(),
                                timeFormat.format(timestampList.get(0)),
                                timeFormat.format(timestampList.get(timestampList.size() - 1)),
                                timeFormat.format(window.getStart()),
                                timeFormat.format(window.getEnd())
                        );
                        out.collect(result);
                    }
                });

        // 把迟到的数据打印到控制台（实际生产中可以保存到其他存储介质中）
        DataStream<Tuple2<String, Long>> sideOutput = window.getSideOutput(outputTag);
        sideOutput.print();

        // 打印结果到控制台
        window.print();

        // flink是懒加载的，所以必须调用execute方法
        env.execute("SteamingWindowWatermarkExample");
    }

}
