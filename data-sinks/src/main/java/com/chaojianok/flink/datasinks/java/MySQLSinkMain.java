package com.chaojianok.flink.datasinks.java;

import com.alibaba.fastjson.JSON;
import com.chaojianok.flink.datasinks.java.model.User;
import com.chaojianok.flink.datasinks.java.sinks.MySQLSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * sink 数据到 mysql
 */
public class MySQLSinkMain {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream(params.get("host"), params.getInt("port"), "\n");

        DataStream<User> user = text
                .map(string -> JSON.parseObject(string, User.class));

        user.addSink(new MySQLSink());

        env.execute("mysql data sink");
    }
}
