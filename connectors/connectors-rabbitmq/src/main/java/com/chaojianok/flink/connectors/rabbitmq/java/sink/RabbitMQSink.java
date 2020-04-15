package com.chaojianok.flink.connectors.rabbitmq.java.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;
import java.util.Map;

public class RabbitMQSink<IN> extends RMQSink<IN> {
    public RabbitMQSink(RMQConnectionConfig rmqConnectionConfig, String queueName, SerializationSchema schema) {
        super(rmqConnectionConfig, queueName, schema);
    }

    protected void setupQueue() throws IOException {
        if (this.queueName != null) {
            this.channel.queueDeclare(this.queueName, true, false, false, (Map) null);
        }
    }
}
