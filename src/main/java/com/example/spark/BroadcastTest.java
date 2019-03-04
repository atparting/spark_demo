package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class BroadcastTest {
    public static void main(String[] args) {
        // 模拟数据源
        Map<Integer, String> dict = new HashMap<>();
        dict.put(1, "cn");
        dict.put(2, "us");

        List<String> source = new ArrayList<>();
        source.add("123");
        source.add("211");
        source.add("198");

        SparkConf conf = new SparkConf().setAppName("BroadcastTest").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        Broadcast<Map<Integer, String>> broadcast = context.broadcast(dict);
        JavaRDD<String> parallelize = context.parallelize(source);
        JavaPairRDD<String, String> pairRDD =
                parallelize.mapToPair(item -> new Tuple2<>(item,
                        broadcast.value().get(Integer.parseInt(item.substring(0, 1)))));
        System.out.println(pairRDD.collect());
    }
}
