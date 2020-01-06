package com.buildupchao.flinkexamples.stream;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author buildupchao
 * @date 2020/01/06 20:25
 * @since JDK 1.8
 */
public class CustomTimestampAndWatermarkExample {

    public static void main(String[] args) {

    }

    private static class PeriodicAssigner implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>> {

        /**
         * 1秒时延设定，表示在1秒以内的数据延时有效，超过一秒的数据被认定为为迟到事件
         */
        private Long maxOutOfOrderness = 1000L;

        private Long currentMaxTimestamp = 0L;

        /**
         * 生成Watermark
         * @return
         */
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            // 根据最大事件时间减去最大的乱序时延长度，然后得到Watermark
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }


        /**
         * 获取当前事件时间
         * @param event
         * @param previousElementTimestamp
         * @return
         */
        @Override
        public long extractTimestamp(Tuple3<String, Long, Integer> event, long previousElementTimestamp) {
            long currentTimestamp = event.f1;
            // 对比当前的事件时间和历史最大事件时间，将最新的时间赋值给currentMaxTimestamp变量
            currentMaxTimestamp = Math.max(currentTimestamp, currentMaxTimestamp);
            return currentTimestamp;
        }
    }

    private static class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<Tuple3<String, Long, Integer>> {


        /**
         * 定义Watermark生成逻辑
         * @param lastElement
         * @param extractedTimestamp
         * @return
         */
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple3<String, Long, Integer> lastElement, long extractedTimestamp) {
            // 根据元素中第三位字段状态是否为0生成Watermark
            if (lastElement.f2 == 0) {
                return new Watermark(extractedTimestamp);
            } else {
                return null;
            }
        }

        /**
         * 定义抽取Timestamp逻辑
         * @param element
         * @param previousElementTimestamp
         * @return
         */
        @Override
        public long extractTimestamp(Tuple3<String, Long, Integer> element, long previousElementTimestamp) {
            return element.f1;
        }
    }
}
