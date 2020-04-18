package com.apache.flink.javatest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class JavaWC {
    public static void main(String[] args) throws Exception {
        // 1. 获取运行环境
        String input = "src\\main\\resources\\hello.txt";
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2. 读取数据
        DataSource<String> text = env.readTextFile(input);
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split(" ");
                for (String token: tokens) {
                    if (token.length() > 0){
                        collector.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();
    }
}
