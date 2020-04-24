package com.chaojianok.flink.connectors.elasticsearch7.java;

import com.alibaba.fastjson.JSONObject;
import com.chaojianok.flink.connectors.elasticsearch7.java.utils.PropertyUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch connector
 */
public class Elasticsearch7ConnectorExample {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String filePath = String.valueOf(Elasticsearch7ConnectorExample.class.getClassLoader().getResource("test_data.txt"));
        DataStream<String> input = env.readTextFile(filePath);

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(PropertyUtil.getValue("elasticsearch.hostname"), Integer.parseInt(PropertyUtil.getValue("elasticsearch.port")), PropertyUtil.getValue("elasticsearch.scheme")));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest indexRequest(String element) {
                        JSONObject jsonObject = JSONObject.parseObject(element);
                        Map<String, String> json = new HashMap<>();
                        json.put("id", jsonObject.getString("id"));
                        json.put("name", jsonObject.getString("name"));
                        json.put("description", jsonObject.getString("description"));
                        json.put("price", jsonObject.getString("price"));
                        return Requests.indexRequest()
                                .index("index_elasticsearch_connector_sink")
                                .source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(indexRequest(element));
                    }
                }
        );

        esSinkBuilder.setBulkFlushMaxActions(1);

        input.addSink(esSinkBuilder.build());

        env.execute("Flink learning connectors elasticsearch7");

    }
}
