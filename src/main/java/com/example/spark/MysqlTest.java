package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MysqlTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();
        String url = "jdbc:mysql://localhost:3306/test?serverTimezone=Asia/Shanghai";
        String table = "user";
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "111111");
        properties.put("driver", "com.mysql.cj.jdbc.Driver");
        Dataset<Row> df = spark.read().jdbc(url, table, properties).select("*");
        df.show();
    }
}
