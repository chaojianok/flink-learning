package com.chaojianok.flink.datasources.java.sources;

import com.chaojianok.flink.datasources.java.model.User;
import com.chaojianok.flink.datasources.java.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * MySQL data source
 */
public class MySQLSource extends RichSourceFunction<User> {

    PreparedStatement ps;

    private Connection connection;

    private String dbDriver = "com.mysql.jdbc.Driver";
    private String dbUrl = "jdbc:mysql://localhost:3306/test?allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    private String dbUser = "root";
    private String dbPassword = "123456";

    /**
     * 获取数据
     *
     * @param sourceContext
     * @throws Exception
     */
    public void run(SourceContext<User> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            User user = new User(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age"));
            sourceContext.collect(user);
        }
    }

    public void cancel() {
    }

    /**
     * 建立数据连接
     *
     * @param parameters
     * @throws Exception
     */
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtil.getConnection(dbDriver, dbUrl, dbUser, dbPassword);
        String sql = "select * from user;";
        ps = connection.prepareStatement(sql);
    }

    /**
     * 关闭数据连接和释放资源
     *
     * @throws Exception
     */
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}
