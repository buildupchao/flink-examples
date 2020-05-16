package com.buildupchao.flinkexamples.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author buildupchao
 * @date 2020/5/16 16:53
 * @since JDK1.8
 **/
public class MysqlSource extends RichSourceFunction<Student> {

    PreparedStatement preparedStatement;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.connection = getConnection();
        String sql = "select * from students";
        preparedStatement = this.connection .prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            ctx.collect(Student.builder()
                    .id(resultSet.getInt("id"))
                    .name(resultSet.getString("name"))
                    .password(resultSet.getString("password"))
                    .age(resultSet.getInt("age"))
                    .build());
        }
    }

    @Override
    public void cancel() {

    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "123456");
        } catch (Exception e) {
            System.out.println("mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}
