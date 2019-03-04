package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class LambdaWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LambdaWordCount");
        // 创建sparkContext
        JavaSparkContext context = new JavaSparkContext(conf);
        // 指定以后从哪里读取数据
        JavaRDD<String> lines = context.textFile(args[0]);
        // 切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        // 将单词和一组合在一起
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));
        // 聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((v1, v2) -> v1 + v2);
        // 调换顺序
        JavaPairRDD<Integer, String> swapped = reduced.mapToPair(Tuple2::swap);
        // 排序
        JavaPairRDD<Integer, String> sorted = swapped.sortByKey(false);
        // 调整顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(Tuple2::swap);
        // 将数据保存到文件
        result.saveAsTextFile(args[1]);
        // 释放资源
        context.stop();
    }
}
