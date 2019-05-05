package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class PartitionTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RddTest").setMaster("local[*]");
        // 数据源
        List<String> source = new ArrayList<>();
        source.add("101 102 101 333 88");
        source.add("555 333 123 999");
        // 创建sparkContext
        JavaSparkContext context = new JavaSparkContext(conf);
        // 指定以后从哪里读取数据
        JavaRDD<String> lines = context.parallelize(source);
        int num = lines.getNumPartitions();
        System.out.println(num);
        JavaRDD<String> coalesce = lines.coalesce(5).cache();
        int num2 = coalesce.getNumPartitions();
        System.out.println(num2);
    }
}
