package com.example.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AccumulatorTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AccumulatorTest").setMaster("local");
        // 创建sparkContext
        JavaSparkContext context = new JavaSparkContext(conf);
        // 模拟数据源
        List<String> source = new ArrayList<>();
        source.add("xiaohua xiaocao dashu");
        source.add("");
        source.add("lantian baiyun xiaohe");
        // 指定以后从哪里读取数据
        // JavaRDD<String> lines = context.textFile(args[0]);
        JavaRDD<String> lines = context.parallelize(source);
                // 累加器
        Accumulator<Integer> accumulator = context.accumulator(0);
        // 切分压平 空行累加
        JavaRDD<String> words = lines.flatMap(line -> {
            if ("".equals(line)) {
                accumulator.add(1);
            }
            return Arrays.asList(line.split(" ")).iterator();
        });
        // 收集
        List<String> collect = words.collect();
        System.out.println(collect);
        System.out.println(accumulator.value());
    }
}
