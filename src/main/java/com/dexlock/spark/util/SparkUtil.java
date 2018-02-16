package com.dexlock.spark.util;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkUtil {
    public SparkSession initializeSpark(){
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("MongoSpark")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/localdb.locations")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/localdb.deviceCountNew")
                .getOrCreate();
        return sparkSession;
    }
}
