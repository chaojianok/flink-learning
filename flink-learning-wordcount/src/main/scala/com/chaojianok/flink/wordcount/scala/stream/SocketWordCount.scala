package com.chaojianok.flink.wordcount.scala.stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * Socket Word Count
 */
object SocketWordCount {
  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val host = params.get("host")
    val port = params.getInt("port")
    val dataStream = env.socketTextStream(host, port)

    val count = dataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    count.print()

    env.execute("Socket Word Count")
  }
}
