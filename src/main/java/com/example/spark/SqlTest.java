package com.example.spark;

import com.example.spark.entity.User;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class SqlTest {
    public static void main(String[] args) {
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

        List<User> list = new ArrayList<>();
        list.add(new User("xiaocao", 18));
        list.add(new User("xiaohua", 17));
        Dataset<Row> userFrame = spark.createDataFrame(list, User.class);
        userFrame.show();
    }
}
