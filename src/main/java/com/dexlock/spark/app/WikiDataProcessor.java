package com.dexlock.spark.app;


import com.dexlock.spark.util.SparkUtil;
import com.dexlock.spark.util.XmlProcessor;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class WikiDataProcessor {
    public static void main(String[] args) {
        SparkUtil sparkUtil = new SparkUtil();
        SparkSession sparkSession = sparkUtil.initializeSpark();
        XmlProcessor.processWikiDump(args[0], sparkSession);
    }
}
