package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.Arrays;

public class MlLibTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> spam = jsc.textFile("files/spam.txt");
        JavaRDD<String> normal = jsc.textFile("files/normal.txt");

        // 创建一个HashingTF实例来把邮件文本映射为包含10000个特征的向量
        HashingTF tf = new HashingTF(10000);

        // 阳性 垃圾邮件
        JavaRDD<LabeledPoint> posExamples = spam.map(email ->
                new LabeledPoint(1, tf.transform(Arrays.asList(email.split(" ")))));
        // 阴性 正常邮件
        JavaRDD<LabeledPoint> negExamples = normal.map(email ->
                new LabeledPoint(0, tf.transform(Arrays.asList(email.split(" ")))));

        JavaRDD<LabeledPoint> trainData = posExamples.union(negExamples);
        trainData.cache(); // 因为逻辑回归是迭代算法，所以缓存训练数据RDD

        // 使用SDG算法运行逻辑回归
        LogisticRegressionModel model = new LogisticRegressionWithSGD().run(trainData.rdd());

        // 以阳性（垃圾邮件）和阴性（正常邮件）的例子分别进行测试
        Vector posTest = tf.transform(
                Arrays.asList("O M G GET cheap stuff by sending money to ...".split(" ")));
        Vector negTest = tf.transform(
                Arrays.asList("Hi Dad, I started studying Spark the other ...".split(" ")));

        System.out.println("Prediction for positive example: " + model.predict(posTest));
        System.out.println("Prediction for negative example: " + model.predict(negTest));
    }
}
