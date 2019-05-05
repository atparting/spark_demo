package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.StatCounter;
import scala.Int;
import scala.Tuple2;

import java.util.*;

public class RddTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RddTest").setMaster("local");
        // 数据源
        List<String> source = new ArrayList<>();
        source.add("101 102 101 333 88");
        source.add("555 333 123 999");
        // 创建sparkContext
        JavaSparkContext context = new JavaSparkContext(conf);
        // 指定以后从哪里读取数据
        JavaRDD<String> lines = context.parallelize(source);
        // 切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        // 转换类型
        JavaRDD<Integer> wordsInt = words.map(Integer::parseInt);
        // 排序
        JavaRDD<Integer> sorted = wordsInt.sortBy(x -> -x, false, 1);
        // 持久化
        sorted.persist(StorageLevel.DISK_ONLY());
        // 收集结果
        List<Integer> collect = sorted.collect();
        System.out.println("collect = " + collect);
        // 元素个数
        long count = sorted.count();
        System.out.println("count = " + count);
        // 每个元素出现的次数
        Map<Integer, Long> countByValue = sorted.countByValue();
        System.out.println("countByValue = " + countByValue);
        // 计算总值
        Integer reduce = sorted.reduce(Integer::sum);
        System.out.println("reduce = " + reduce);
        // 采样
        JavaRDD<Integer> sample = sorted.sample(false, 0.5);
        System.out.println("sample = " + sample.collect());
        // 转化为DoubleRDD
        JavaDoubleRDD doubleRDD = sorted.mapToDouble(x -> 1.0 * x);
        StatCounter stats = doubleRDD.stats();
        // 平均值 最小值 方差 标准差
        System.out.println("mean = " + stats.mean());
        System.out.println("min = " + stats.min());
        System.out.println("variance = " + stats.variance());
        System.out.println("stdev = " + stats.stdev());


//        // 转化成pairRDD
//        JavaPairRDD<Integer, Integer> pairRDD = sorted.mapToPair(word -> new Tuple2<>(word, 1));
//        System.out.println("pairRDD = " + pairRDD.collect());
//        // 每个元素出现的次数
//        JavaPairRDD<Integer, Integer> reduceByKey = pairRDD.reduceByKey((v1, v2) -> v1 + v2);
//        System.out.println("reduceByKey = " + reduceByKey.collect());
//        // 对相同键的值分组
//        JavaPairRDD<Integer, Iterable<Integer>> grouped = pairRDD.groupByKey();
//        System.out.println("grouped = " + grouped.collect());
//        // 对值操作
//        JavaPairRDD<Integer, Double> mapValues = pairRDD.mapValues(value -> 1.0 * value * value);
//        System.out.println("mapValues = " + mapValues.collectAsMap());
//        // 合并
//        JavaPairRDD<Integer, Double> combined = pairRDD.combineByKey(valueBefore -> Double.parseDouble(valueBefore.toString()),
//                (valueAfter, valueBefore) -> valueAfter + valueBefore,
//                (valueAfter1, valueAfter2) -> valueAfter1 + valueAfter2);
//        System.out.println("combine = " + combined.collect());
//        // 排序
////        JavaPairRDD<Integer, Integer> pairSorted = pairRDD.sortByKey(Comparator.comparing(String::valueOf));
////        System.out.println("pairSorted = " + pairSorted.collect());
//        // 过滤
//        JavaPairRDD<Integer, Integer> filtered = pairRDD.filter(tp -> tp._1 > 100);
//        System.out.println("filtered = " + filtered.collect());
//        // 给定键的所有值
//        List<Integer> lookup = pairRDD.lookup(333);
//        System.out.println("lookup = " + lookup);
        context.stop();
    }
}
