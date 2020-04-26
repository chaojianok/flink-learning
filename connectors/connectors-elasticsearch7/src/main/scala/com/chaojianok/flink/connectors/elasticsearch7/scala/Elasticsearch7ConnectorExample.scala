package com.chaojianok.flink.connectors.elasticsearch7.scala

import java.util

import com.alibaba.fastjson.JSON
import com.chaojianok.flink.connectors.elasticsearch7.scala.utils.PropertyUtil
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * Elasticsearch connectors
 */
object Elasticsearch7ConnectorExample {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val filePath = Elasticsearch7ConnectorExample.getClass.getClassLoader.getResource("test_data.txt").toString
    val input = env.readTextFile(filePath)

    val httpHosts = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost(PropertyUtil.getValue("elasticsearch.hostname"), PropertyUtil.getValue("elasticsearch.port").toInt, PropertyUtil.getValue("elasticsearch.scheme")))

    val esSinkBuilder = new ElasticsearchSink.Builder[String](httpHosts, new ElasticsearchSinkFunction[String]() {
      def indexRequest(element: String): IndexRequest = {
        val jsonObject = JSON.parseObject(element)
        val json = new util.HashMap[String, String]
        json.put("id", jsonObject.getString("id"))
        json.put("name", jsonObject.getString("name"))
        json.put("description", jsonObject.getString("description"))
        json.put("price", jsonObject.getString("price"))
        Requests.indexRequest.index("index_elasticsearch_connector_sink").source(json)
      }

      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        indexer.add(indexRequest(element))
      }
    })

    esSinkBuilder.setBulkFlushMaxActions(1)

    input.addSink(esSinkBuilder.build)

    env.execute("Flink learning connectors elasticsearch7")
  }
}
