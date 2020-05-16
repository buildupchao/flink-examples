package com.buildupchao.flinkexamples.api;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * @author buildupchao
 * @date 2020/01/01 20:23
 * @since JDK 1.8
 */
public class DistributedCacheExample {

    private static final String CACHE_FILE_NAME = "CACHE_DATA";

    /**
     * Flink提供了类似于Apache Hadoop的分布式缓存，可以让并行用户函数实例化本地化的访问文件。此功能可用于共享
     * 包含静态外部数据（如字典或机器学习的回归模型）的文件。
     * <p>
     * 工作方式如下：程序将本地或远程文件系统（如HDFS或S3）的文件或目录作为缓存文件注册到ExecutionEnvironment中的
     * 特定名称下。当程序执行时，Flink自动将文件或目录复制到所有worker的本地文件系统。用户函数可以查找指定名称下的
     * 文件或目录，并从worker的本地文件系统访问它。
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = System.getProperty("user.dir") + "/data/cacheData.txt";
        // register a local executable file (script, executable, ...)
        environment.registerCachedFile("file://" + filePath, CACHE_FILE_NAME, true);

        // read source data
        DataSource<Tuple2<Integer, Long>> earnMoneyDataSource = environment.fromCollection(Lists.newArrayList(
                Tuple2.of(101, 3_000_000_000L),
                Tuple2.of(102, 4_000_000_000L),
                Tuple2.of(103, 5_000_000_000L)
        ));

        DataSet<String> result = earnMoneyDataSource.map(new RichMapFunction<Tuple2<Integer, Long>, String>() {

            Map<Integer, String> userIdNameMap = new HashMap<>(16);

            /**
             * read data of cache file
             *
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // access cached file via RuntimeContext and DistributedCache
                File localFile = getRuntimeContext().getDistributedCache().getFile(CACHE_FILE_NAME);
                for (String line : FileUtils.readLines(localFile)) {
                    String[] userInfos = line.split(",");
                    userIdNameMap.put(Integer.parseInt(userInfos[0]), userInfos[1]);
                }
            }

            /**
             * using data of cache file
             *
             * @param tuple2
             * @return
             * @throws Exception
             */
            @Override
            public String map(Tuple2<Integer, Long> tuple2) throws Exception {
                String userName = userIdNameMap.getOrDefault(tuple2.f0, "unknown");
                return String.format("user[userId=%d, name=%s] earns %d money.", tuple2.f0, userName, tuple2.f1);
            }
        });

        result.print();
    }
}
