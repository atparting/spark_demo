package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SqlTest {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setAppName("sqlTest").setMaster("local");
//        // 创建sparkContext
//        JavaSparkContext sparkContext = new JavaSparkContext(conf);
//        // 创建SQLContext
//        SQLContext sqlContext = new SQLContext(sparkContext);
//        // 读json文件
//        Dataset<Row> people = sqlContext.read().json("C:\\Users\\admin\\Downloads\\people");
//        people.registerTempTable("people");
//        Dataset<Row> df = sqlContext.sql("select name, age from people");
//        df.show();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read().json("C:\\Users\\admin\\Downloads\\people");
        df.show();
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDf = spark.sql("select name from people where age = 17");
        sqlDf.show();
    }
}
