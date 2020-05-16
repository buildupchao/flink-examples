package com.buildupchao.flinkexamples.dbus.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author buildupchao
 * @date 2020/02/04 16:14
 * @since JDK 1.8
 */
@Slf4j
public class HbaseTemplate implements Serializable {

    private static final long serialVersionUID = 5873215348692967381L;

    private Configuration configuration;
    private Connection connection;

    public HbaseTemplate(Configuration configuration) {
        this.configuration = configuration;
        initConnection();
    }

    private void initConnection() {
        try {
            this.connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() {
        if (connection == null || connection.isAborted() || connection.isClosed()) {
            initConnection();
        }
        return connection;
    }

    public boolean tableExists(String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin) getConnection().getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable(String tableName, String... familyNames) {
        try (HBaseAdmin admin = (HBaseAdmin) getConnection().getAdmin()) {
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            // 添加列簇
            if (familyNames != null) {
                for (String familyName : familyNames) {
                    HColumnDescriptor hcd = new HColumnDescriptor(familyName);
                    desc.addFamily(hcd);
                }
            }
            admin.createTable(desc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void disableTable(String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin) getConnection().getAdmin()) {
            admin.disableTable(tableName);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void deleteTable(String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin) getConnection().getAdmin()) {
            if (admin.isTableEnabled(tableName)) {
                disableTable(tableName);
            }
            admin.deleteTable(tableName);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 插入一行数据
     *
     * @param tableName
     * @param hRow
     * @return
     */
    public Boolean put(String tableName, HRow hRow) {
        boolean flag = false;

        try {
            HTable table = (HTable) getConnection().getTable(TableName.valueOf(tableName));
            Put put = new Put(hRow.getRowKey());
            for (HRow.HCell hCell : hRow.getCells()) {
                put.addColumn(Bytes.toBytes(hCell.getFamily()), Bytes.toBytes(hCell.getQualifier()), hCell.getValue());
            }
            table.put(put);
            flag = true;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return flag;
    }

    /**
     * 批量插入数据
     *
     * @param tableName
     * @param rows
     * @return
     */
    public Boolean puts(String tableName, List<HRow> rows) {
        boolean flag = false;

        try {
            HTable table = (HTable) getConnection().getTable(TableName.valueOf(tableName));
            List<Put> puts = new ArrayList<>();

            for (HRow hRow : rows) {
                Put put = new Put(hRow.getRowKey());
                for (HRow.HCell hCell : hRow.getCells()) {
                    put.addColumn(Bytes.toBytes(hCell.getFamily()), Bytes.toBytes(hCell.getQualifier()), hCell.getValue());
                }
                puts.add(put);
            }
            if (CollectionUtils.isNotEmpty(puts)) {
                table.put(puts);
            }
            flag = true;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return flag;
    }

    /**
     * 批量十三处数据
     *
     * @param tableName
     * @param rowKeys
     * @return
     */
    public Boolean deletes(String tableName, Set<byte[]> rowKeys) {
        boolean flag = false;

        try {
            HTable table = (HTable) getConnection().getTable(TableName.valueOf(tableName));
            List<Delete> deletes = new ArrayList<>();
            for (byte[] rowKey : rowKeys) {
                deletes.add(new Delete(rowKey));
            }
            if (CollectionUtils.isNotEmpty(deletes)) {
                table.delete(deletes);
            }
            flag = true;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return flag;
    }

    public void close() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }
}
