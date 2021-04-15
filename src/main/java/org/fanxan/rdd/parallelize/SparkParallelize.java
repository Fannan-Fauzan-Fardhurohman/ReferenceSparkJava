package org.fanxan.rdd.parallelize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class SparkParallelize {
    public static void main(String[] args) {
//        SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local[2]");
        SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // sample collection
        List<Integer> collection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
//        JavaRDD<Integer> rdd = sparkContext.parallelize(collection, 2);
        JavaRDD<Integer> rdd = sparkContext.parallelize(collection);
        System.out.println("Number Of Partition "+rdd.getNumPartitions());

        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}
