package com.buildupchao.flinkexamples.batch.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author buildupchao
 * @date 2020/01/14 00:08
 * @since JDK 1.8
 */
public class BatchOrderCaseSQLExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(executionEnvironment);

        DataSet<String> dataSet = executionEnvironment.readTextFile("data/order.txt");
        DataSet<Tuple6<String, String, Integer, Double, Integer, Integer>> ds2 = dataSet.map(new MapFunction<String, Tuple6<String, String, Integer, Double, Integer, Integer>>() {
            @Override
            public Tuple6<String, String, Integer, Double, Integer, Integer> map(String value) throws Exception {
                String[] segments = value.split(",");
                return new Tuple6<>(
                        segments[0],
                        segments[1],
                        Integer.parseInt(segments[2]),
                        Double.parseDouble(segments[3]),
                        Integer.parseInt(segments[4]),
                        Integer.parseInt(segments[5])
                );
            }
        });

        // 这里的name不能命名为`Order`，因为其为Flink保留关键字
        // Flink保留关键字链接：https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html
        tableEnvironment.registerDataSet("Orders", ds2, "orderNo,userId,orderStatus,goodsMoney,payType,payFrom");

        Table table = tableEnvironment.sqlQuery(
                "select userId,sum(goodsMoney) as allMoney from Orders where orderStatus=1 group by userId"
        );
        DataSet<Row> rowDataSet = tableEnvironment.toDataSet(table, Row.class);
        rowDataSet.print();
    }
}
