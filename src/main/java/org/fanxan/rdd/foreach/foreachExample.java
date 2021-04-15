package org.fanxan.rdd.foreach;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class foreachExample {
    public static void main(String[] args) {
        SparkConf sparkConf= new SparkConf().setMaster("local[3]").setAppName("Foreach");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

//        Read List To Rdd
        List<String> data = Arrays.asList("Learn", "Apache", "Spark");
        JavaRDD<String> items = sparkContext.parallelize(data, 1);

        items.foreach(item -> {
            System.out.println("* "+item);
        });
    }
}
