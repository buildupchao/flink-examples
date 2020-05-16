package com.buildupchao.flinkexamples.connector.util;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.hadoop.shaded.org.apache.http.HttpHost;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * @author buildupchao
 * @date 2020/5/16 17:31
 * @since JDK1.8
 **/
public class EsSinkUtil {
    /**
     * elasticsearch security constant
     */
    public static final String ES_SECURITY_ENABLE = "es.security.enable";
    public static final String ES_SECURITY_USERNAME = "es.security.username";
    public static final String ES_SECURITY_PASSWORD = "es.security.password";


    public static <T> void addSink(List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
                                   SingleOutputStreamOperator<T> data, ElasticsearchSinkFunction<T> func,
                                   ParameterTool parameterTool) {
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder(hosts, func);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        esSinkBuilder.setFailureHandler(new RetryRequestFailureHandler());
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }

    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        List<HttpHost> addressList = Lists.newArrayList();
        for (String host : hosts.split(",")) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addressList.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addressList.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addressList;
    }
}
