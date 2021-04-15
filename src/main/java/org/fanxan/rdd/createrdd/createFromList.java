package org.fanxan.rdd.createrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class createFromList {
    public static void main(String[] args) {
        SparkConf sparkConf= new SparkConf().setAppName("Spark Rdd foreach example").setMaster("local[2]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

//        Read List To Rdd
        List<String> data = Arrays.asList("Fannan", "Fauzan", "Fardhurohman");
        JavaRDD<String> items = sparkContext.parallelize(data, 1);

//        apply a function for each elements of RDD
        items.foreach(item -> {
            System.out.println("* "+item);
        });
    }
}
