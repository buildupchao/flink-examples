package com.buildupchao.flinkexamples.dbus.sink;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.buildupchao.flinkexamples.batch.dbus.config.GlobalConfig;
import com.buildupchao.flinkexamples.batch.dbus.model.Flow;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @author buildupchao
 * @date 2020/02/05 18:20
 * @since JDK 1.8
 */
@Slf4j
public class HbaseSyncSink extends RichSinkFunction<Tuple2<FlatMessage, Flow>> {

    private HbaseSyncService hbaseSyncService;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", GlobalConfig.HBASE_ZOOKEEPER_QUORUM);
        configuration.set("hbase.zookeeper.property.clientPort", GlobalConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
        configuration.set("zookeeper.znode.parent", GlobalConfig.ZOOKEEPER_ZNODE_PARENT);

        HbaseTemplate hbaseTemplate = new HbaseTemplate(configuration);
        hbaseSyncService = new HbaseSyncService(hbaseTemplate);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(Tuple2<FlatMessage, Flow> value, Context context) throws Exception {
        hbaseSyncService.sync(value.f1, value.f0);
    }
}
