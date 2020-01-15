package com.chaojianok.flink.wordcount.java.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Socket Word Count
 */
public class SocketWordCount {
    public static void main(String[] args) throws Exception {

        // 获取参数
        ParameterTool params = ParameterTool.fromArgs(args);

        // 创建数据流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过连接socket获取输入数据
        DataStream<String> text = env.socketTextStream(params.get("host"), params.getInt("port"), "\n");

        // 计算
        DataStream<Tuple2<String, Integer>> windowCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // 将结果打印到控制台
        windowCounts.print();

        // 执行程序
        env.execute("Socket Window WordCount");
    }
}
