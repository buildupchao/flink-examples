package com.buildupchao.flinkexamples.dbus.function;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.buildupchao.flinkexamples.batch.dbus.enums.FlowStatusEnum;
import com.buildupchao.flinkexamples.batch.dbus.incrementsync.IncrementSyncApp;
import com.buildupchao.flinkexamples.batch.dbus.model.Flow;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 处理方法
 *
 * @author buildupchao
 * @date 2020/02/02 21:10
 * @since JDK 1.8
 */
public class DbusProcessFunction extends KeyedBroadcastProcessFunction<String, FlatMessage, Flow, Tuple2<FlatMessage, Flow>> {

    @Override
    public void processElement(FlatMessage value, ReadOnlyContext ctx, Collector<Tuple2<FlatMessage, Flow>> out) throws Exception {
        // 获取配置流
        Flow flow = ctx.getBroadcastState(IncrementSyncApp.flowStateDescriptor).get(value.getDatabase() + value.getTable());

        if (null != flow && flow.getStatus() == FlowStatusEnum.FLOW_STATUS_RUNNING.getCode()) {
            out.collect(Tuple2.of(value, flow));
        }
    }

    @Override
    public void processBroadcastElement(Flow flow, Context ctx, Collector<Tuple2<FlatMessage, Flow>> out) throws Exception {
        // 获取state状态
        BroadcastState<String, Flow> broadcastState = ctx.getBroadcastState(IncrementSyncApp.flowStateDescriptor);

        // 更新state
        broadcastState.put(flow.getDatabaseName() + flow.getTableName(), flow);
    }
}
