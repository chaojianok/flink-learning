package com.chaojianok.flink.datasources.java;

import com.chaojianok.flink.datasources.java.sources.MySQLSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * mysql data source
 */
public class MySQLSourceMain {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new MySQLSource()).print();

        env.execute("MySQL data source");
    }
}
