package com.dexlock.spark.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.File;

public class XmlProcessor {
    public static void processXmlFile(String xmlFilePath, SparkSession sparkSession) {
        Dataset<Row> xmlDataset = sparkSession.sqlContext().read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "book")
                .load(xmlFilePath);
        xmlDataset.show(50);

        if (!new File("newbooks.xml").exists()) {
            xmlDataset.select("author", "_id").write()
                    .format("com.databricks.spark.xml")
                    .option("rootTag", "books")
                    .option("rowTag", "book")
                    .save("newbooks.xml");
        }
    }

    public static void processWikiDump(String filePath, SparkSession sparkSession){
       /* Dataset<Row> wikiDataSet = sparkSession.sqlContext().read()
                .format("com.databricks.spark.xml")
                //.option("rowTag", "page")
                .option("valueTag", "page")
                .load(filePath);
        wikiDataSet.show(30);*/

       sparkSession.sql("CREATE TABLE wikidata (_id string, _title string, _revision string, _type string, page string)\n" +
               "USING com.databricks.spark.xml\n" +
               "OPTIONS (path \""+filePath+"\", rootTag \"page\")");
       Dataset<Row> wikiDataSet = sparkSession.sql("SELECT * FROM wikidata limit 100");
       wikiDataSet.show(30);
    }

}
