package com.buildupchao.flinkexamples.streaming;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.List;

/**
 * @author buildupchao
 * @date 2020/01/06 19:29
 * @since JDK 1.8
 */
public class SourceFunctionUsedForEventTimeAndWatermarksExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<String, Long, Integer>> input = Lists.newArrayList(
                Tuple3.of("a", 1L, 1),
                Tuple3.of("b", 1L, 1),
                Tuple3.of("b", 3L, 1)
        );
        // 添加DataSource数据源，实例化SourceFunction接口
        // 在SourceFunction中直接定义Timestamps和Watermarks
        environment.addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
            @Override
            public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
                input.forEach(value -> {
                    // 调用collectWithTimestamp增加Event Time抽取
                    ctx.collectWithTimestamp(value, value.f1);
                    // 调用emitWatermark，创建Watermark，最大延时设定为1
                    ctx.emitWatermark(new Watermark(value.f1 - 1));
                });
                // 设定默认Watermark
                ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
            }

            @Override
            public void cancel() {

            }
        });
    }
}
