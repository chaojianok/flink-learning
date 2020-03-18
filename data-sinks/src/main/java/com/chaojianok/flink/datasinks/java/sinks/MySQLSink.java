package com.chaojianok.flink.datasinks.java.sinks;

import com.chaojianok.flink.datasinks.java.model.User;
import com.chaojianok.flink.datasinks.java.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * mysql data sink
 */
public class MySQLSink extends RichSinkFunction<User> {

    PreparedStatement ps;

    private Connection connection;

    private String dbDriver = "com.mysql.jdbc.Driver";
    private String dbUrl = "jdbc:mysql://localhost:3306/test?allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    private String dbUser = "root";
    private String dbPassword = "123456";

    /**
     * 建立连接
     *
     * @param parameters
     * @throws Exception
     */
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtil.getConnection(dbDriver, dbUrl, dbUser, dbPassword);
        String sql = "insert into Student (id, name, password, age) values (null, ?, ?, ?);";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    /**
     * 关闭数据连接和释放资源
     *
     * @throws Exception
     */
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    public void invoke(User value, Context context) throws Exception {
        if (ps == null) {
            return;
        }
        //组装数据，执行插入操作
        ps.setInt(1, value.getId());
        ps.setString(2, value.getName());
        ps.setString(3, value.getPassword());
        ps.setInt(4, value.getAge());
        ps.executeUpdate();
    }
}
