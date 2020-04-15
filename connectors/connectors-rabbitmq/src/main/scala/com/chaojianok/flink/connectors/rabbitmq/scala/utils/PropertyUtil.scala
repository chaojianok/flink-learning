package com.chaojianok.flink.connectors.rabbitmq.scala.utils

import java.io.FileInputStream
import java.util.Properties

object PropertyUtil {

  def loadProperties(): Properties = {
    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource("application.properties").getPath
    properties.load(new FileInputStream(path))
    return properties
  }

  def getValue(key: String): String = {
    return loadProperties.getProperty(key)
  }

}
