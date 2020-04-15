package com.chaojianok.flink.connectors.rabbitmq.scala.sink

import java.io.IOException

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig


class RabbitMQSink[IN](rmqConnectionConfig: RMQConnectionConfig, queueName: String, schema: SerializationSchema[IN]) extends RMQSink[IN](rmqConnectionConfig, queueName, schema) {
  @throws[IOException]
  override protected def setupQueue(): Unit = {
    if (this.queueName != null) this.channel.queueDeclare(this.queueName, true, false, false, null)
  }
}
