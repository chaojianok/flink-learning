package com.chaojianok.flink.wordcount.java.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Word Count
 */
public class WordCount {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取文件里的单词
        String filePath = WordCount.class.getClassLoader().getResource("WordCount.txt").toString();
        DataSet<String> dataSet = env.readTextFile(filePath);

        // 计算
        DataSet<Tuple2<String, Integer>> count = dataSet
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .groupBy(0)
                .sum(1);

        // 输出到控制台
        count.print();
    }
}
