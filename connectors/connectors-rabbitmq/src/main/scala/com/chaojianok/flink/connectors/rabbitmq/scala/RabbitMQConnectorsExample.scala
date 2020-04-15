package com.chaojianok.flink.connectors.rabbitmq.scala

import com.chaojianok.flink.connectors.rabbitmq.scala.sink.RabbitMQSink
import com.chaojianok.flink.connectors.rabbitmq.scala.utils.PropertyUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig

/**
 * RabbitMQ connectors
 */
object RabbitMQConnectorsExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val connectionConfig = new RMQConnectionConfig.Builder()
      .setHost(PropertyUtil.getValue("rabbitmq.host"))
      .setPort(Integer.valueOf(PropertyUtil.getValue("rabbitmq.port")))
      .setUserName(PropertyUtil.getValue("rabbitmq.username"))
      .setPassword(PropertyUtil.getValue("rabbitmq.password"))
      .setVirtualHost(PropertyUtil.getValue("rabbitmq.virtual-host"))
      .build()

    val stream = env
      .addSource(new RMQSource[String](
        connectionConfig,
        "rabbitmq_connectors",
        true,
        new SimpleStringSchema()))
      .setParallelism(1)

    //stream.print().name("RabbitMQ connectors");
    stream.addSink(new RabbitMQSink[String](
      connectionConfig,
      "rabbitmq_connectors_sink",
      new SimpleStringSchema()))

    env.execute("RabbitMQ connectors")
  }
}
