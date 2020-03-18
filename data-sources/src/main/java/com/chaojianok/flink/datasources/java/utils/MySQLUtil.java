package com.chaojianok.flink.datasources.java.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * MySQL工具类
 */
public class MySQLUtil {

    private static final Logger logger = LoggerFactory.getLogger(MySQLUtil.class);

    public static Connection getConnection(String driver, String url, String user, String password) {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            logger.warn("======>> mysql get connection exception", e);
        }
        return con;
    }
}
