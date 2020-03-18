package com.chaojianok.flink.wordcount.scala.batch

import org.apache.flink.api.scala._

/**
 * Word Count
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePath = WordCount.getClass.getClassLoader.getResource("WordCount.txt").toString
    val dataSet = env.readTextFile(filePath)

    val count = dataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    count.print()
  }
}
