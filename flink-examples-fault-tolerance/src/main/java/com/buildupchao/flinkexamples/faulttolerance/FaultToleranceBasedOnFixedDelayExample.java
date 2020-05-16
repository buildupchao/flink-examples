package com.buildupchao.flinkexamples.faulttolerance;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 批处理容错
 *
 * @author buildupchao
 * @date 2020/01/02 00:42
 * @since JDK 1.8
 */
public class FaultToleranceBasedOnFixedDelayExample {

    /**
     * <p>
     * <h2>Flink DataSet容错</h2>
     * <p>
     * 批处理容错是基于失败重试实现的
     * <p>
     * 重启策略：1.固定延时，2.失败率，3.无重启
     *
     * <table><thead><tr><th>Restart Strategy</th><th>配置项</th><th>说明</th></tr></thead>
     *     <tbody>
     *         <tr>
     *             <td>固定延迟（Fixed delay）</td>
     *             <td>
     *                 restart-strategy:fixed-delay；
     *                 restart-strategy.attempts:3 （默认值:1）；
     *                 restart-strategy.fixed-delay.delay:10s（默认值：akka.ask.timeout）
     *             </td>
     *             <td>
     *                 如果超过最大尝试次数，作业最终会失败。在连续两次重启尝试之间等待固定的时间。
     *             </td>
     *         </tr>
     *         <tr>
     *             <td>失败率（Failure rate）</td>
     *             <td>
     *                 restart-strategy:failure-rate；
     *                 restart-strategy.failure-rate.max-failure-per-interval:3（默认值：1）；
     *                 restart-strategy.failure-rate.failure-rate-interval: 5 min（默认值：1 minute）
     *             </td>
     *             <td>
     *                 当失败后重新启动作业，但是当超过故障率（每个时间间隔的故障）时，作业最终会失败。
     *                 在连续两次重启尝试之间等待固定的时间。
     *             </td>
     *         </tr>
     *         <tr>
     *             <td>无重启（No restart）</td>
     *             <td>
     *                 restart-strategy: none
     *             </td>
     *             <td>-</td>
     *         </tr>
     *     </tbody>
     * </table>
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 失败最多尝试重启3次，连续两次重启尝试之间等待时间为3秒钟
        // 超过3次重启尝试将会任务失败
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            3,
                Time.of(3, TimeUnit.SECONDS)
        ));

        DataSet<String> dataSet = environment.fromElements("6", "7", "", "9", "10");
        dataSet.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                // 打印信息用于放大效果
                System.out.println(s);

                return Integer.parseInt(s);
            }
        }).print();
    }
}
