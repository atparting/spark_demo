package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ReadJson {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("readJson").setMaster("local");
        // 创建sparkContext
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> input = context.textFile("C:\\Users\\admin\\Downloads\\people");
        JavaRDD<People> result = input.mapPartitions((lines) -> {
            List<People> peoples = new ArrayList<>();
            ObjectMapper mapper = new ObjectMapper();
            while (lines.hasNext()) {
                String line = lines.next();
                People people = mapper.readValue(line, People.class);
                peoples.add(people);
            }
            return peoples.iterator();
        });
        List<People> collect = result.collect();
        System.out.println(collect.get(0));
    }

    static class People implements Serializable {
        private String name;
        private Integer age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "People{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
