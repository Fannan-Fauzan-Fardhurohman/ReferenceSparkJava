package org.fanxan.rdd.ReadJsonToRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadJsonToRdd {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("spark read json").master("local[2]").getOrCreate();
        String jsonPath = "D:/PersonalProject/Backend/jsonShopee.json";
        JavaRDD<Row> items = sparkSession.read().json(jsonPath).toJavaRDD();

        items.foreach(e ->{
            System.out.println(e);
        });
    }
}
