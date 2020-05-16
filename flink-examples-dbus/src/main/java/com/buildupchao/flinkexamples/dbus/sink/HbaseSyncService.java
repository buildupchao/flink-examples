package com.buildupchao.flinkexamples.dbus.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.buildupchao.flinkexamples.batch.dbus.model.Flow;
import com.buildupchao.flinkexamples.batch.dbus.utils.Md5Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HBase同步操作业务
 *
 * @author buildupchao
 * @date 2020/02/05 12:11
 * @since JDK 1.8
 */
@Slf4j
public class HbaseSyncService implements Serializable {

    private static final long serialVersionUID = -1972054481105611073L;

    private static final String INSERT = "INSERT";
    private static final String UPDATE = "UPDATE";
    private static final String DELETE = "DELETE";

    private HbaseTemplate hbaseTemplate;

    public HbaseSyncService(HbaseTemplate hbaseTemplate) {
        this.hbaseTemplate = hbaseTemplate;
    }

    public void sync(Flow flow, FlatMessage dml) {
        if (flow == null) {
            return;
        }
        String type = dml.getType();
        if (type != null) {
            if (INSERT.equalsIgnoreCase(type)) {
                insert(flow, dml);
            } else if (UPDATE.equalsIgnoreCase(type)) {
//                update(flow, dml);
            } else if (DELETE.equalsIgnoreCase(type)) {
//                delete(flow, dml);
            }
            if (log.isDebugEnabled()) {
                log.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }
    }

    /**
     * 插入操作
     *
     * @param flow 配置项
     * @param dml  DML数据
     */
    private void insert(Flow flow, FlatMessage dml) {
        List<Map<String, String>> data = dml.getData();
        if (CollectionUtils.isEmpty(data)) {
            return;
        }

        int i = 1;
        boolean complete = false;
        List<HRow> rows = new ArrayList<>();
        for (Map<String, String> r : data) {
            HRow hRow = new HRow();

            // 拼接复合rowKey
            if (flow.getRowKey() != null) {
                String[] rowKeyColumns = flow.getRowKey().trim().split(",");
                String rowKeyValue = getRowKey(rowKeyColumns, r);
                hRow.setRowKey(Bytes.toBytes(rowKeyValue));
            }

            convertData2Row(flow, hRow, r);
            if (hRow.getRowKey() == null) {
                throw new RuntimeException("empty rowKey: " + hRow.toString() + ", Flow: " + flow.toString());
            }
            rows.add(hRow);
            complete = true;

            if (i % flow.getCommitBatch() == 0 && !rows.isEmpty()) {
                hbaseTemplate.puts(flow.getHbaseTable(), rows);
                rows.clear();
                complete = true;
            }
            i++;
        }
        if (!complete && !rows.isEmpty()) {
            hbaseTemplate.puts(flow.getHbaseTable(), rows);
        }
    }

    /**
     * 获取复合字段作为rowKey的拼接
     *
     * @param rowKeyColumns 复合rowKey对应的字段
     * @param data             数据
     * @return
     */
    private String getRowKey(String[] rowKeyColumns, Map<String, String> data) {
        StringBuilder rowKeyValue = new StringBuilder();
        for (String rowKeyColumnName : rowKeyColumns) {
            Object obj = data.get(rowKeyColumnName);
            if (obj != null) {
                rowKeyValue.append(obj.toString());
            }
            rowKeyValue.append("|");
        }
        int len = rowKeyValue.length();
        if (len > 0) {
            rowKeyValue.delete(len - 1, len);
        }

        // 可自行扩展支持多种rowKey生成策略，这里写死为md5前缀
        return Md5Utils.getMd5String(rowKeyValue.toString()).substring(0, 8) + "_" + rowKeyValue.toString();
    }

    /**
     * 将Map数据转换为HRow行数据
     *
     * @param flow hbase映射配置
     * @param hRow 行数据
     * @param data Map数据
     */
    private void convertData2Row(Flow flow, HRow hRow, Map<String, String> data) {
        String familyName = flow.getFamily();

        for (Map.Entry<String, String> entry : data.entrySet()) {
            if (entry.getValue() != null) {
                byte[] bytes = Bytes.toBytes(entry.getValue());

                String qualifier = entry.getKey();
                if (flow.isUppercaseQualifier()) {
                    qualifier = qualifier.toUpperCase();
                }
                hRow.addCell(familyName, qualifier, bytes);
            }
        }
    }
}
