package com.buildupchao.flinkexamples.dbus.config;

import java.io.Serializable;

/**
 * <p>
 *     后续可以更换为配置中心，比如<strong>nacos</strong>
 * </p>
 *
 * @author buildupchao
 * @date 2020/02/02 18:41
 * @since JDK 1.8
 */
public class GlobalConfig implements Serializable {

    /**
     * MySQL配置
     */
    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    public static final String DB_URL = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8";
    public static final String USER_NAME = "canal";
    public static final String PASSWORD = "canal";
    /**
     * 批量提交size
     */
    public static final int BATCH_SIZE = 2;

    /**
     * HBase相关配置
     */
    public static final String HBASE_ZOOKEEPER_QUORUM = "localhost";
    public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181";
    public static final String ZOOKEEPER_ZNODE_PARENT = "/hbase";
}
