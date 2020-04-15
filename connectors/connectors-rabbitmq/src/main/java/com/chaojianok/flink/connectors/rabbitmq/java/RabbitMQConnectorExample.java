package com.chaojianok.flink.connectors.rabbitmq.java;

import com.chaojianok.flink.connectors.rabbitmq.java.sink.RabbitMQSink;
import com.chaojianok.flink.connectors.rabbitmq.java.utils.PropertyUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * RabbitMQ connectors
 */
public class RabbitMQConnectorExample {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(PropertyUtil.getValue("rabbitmq.host"))
                .setPort(Integer.valueOf(PropertyUtil.getValue("rabbitmq.port")))
                .setUserName(PropertyUtil.getValue("rabbitmq.username"))
                .setPassword(PropertyUtil.getValue("rabbitmq.password"))
                .setVirtualHost(PropertyUtil.getValue("rabbitmq.virtual-host"))
                .build();

        final DataStream<String> stream = env
                .addSource(new RMQSource<>(
                        connectionConfig,
                        "rabbitmq_connectors",
                        true,
                        new SimpleStringSchema()))
                .setParallelism(1);

        //stream.print().name("RabbitMQ connectors");
        stream.addSink(new RabbitMQSink<>(
                connectionConfig,
                "rabbitmq_connectors_sink",
                new SimpleStringSchema()));

        env.execute("Flink learning connectors rabbitmq");

    }
}
