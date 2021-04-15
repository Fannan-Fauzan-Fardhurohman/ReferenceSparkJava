package org.fanxan.rdd.createrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateFromJsonFile {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("spark json").master("local[2]").getOrCreate();

        // read list to rdd
        String jsonPath = "D:/PersonalProject/Backend/jsonShopee.json";
        JavaRDD<Row> items = sparkSession.read().json(jsonPath).toJavaRDD();
        items.foreach(item -> {
            System.out.println(item);
        });
    }
}
