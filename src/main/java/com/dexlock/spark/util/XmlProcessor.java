package com.dexlock.spark.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.xml.XML$;

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
        Dataset<Row> wikiDataSet1 = sparkSession.sqlContext().read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "pages")
                .load(filePath);
        //wikiDataSet1.show(30);
       // System.out.println("****\n");
       // wikiDataSet1.printSchema();
        JavaRDD expandedWikiRDD = wikiDataSet1.javaRDD().map(row->{
            return row.get(0);
        });
        System.out.println(expandedWikiRDD.top(1));
        StructType customSchema = new StructType(new StructField[] {
                new StructField("_VALUE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("_id", DataTypes.LongType, true, Metadata.empty()),
                new StructField("_ns-id", DataTypes.LongType, true, Metadata.empty()),
                new StructField("_ns-name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("_revision", DataTypes.LongType, true, Metadata.empty()),
                new StructField("_title", DataTypes.StringType, true, Metadata.empty()),
                new StructField("_type", DataTypes.StringType, true, Metadata.empty())
        });
  /*     sparkSession.sql("CREATE TABLE wikidata (_id string, _title string, _revision string, _type string, _page string, _nsid string, _nsname string, description string)\n" +
               "USING com.databricks.spark.xml\n" +
               "OPTIONS (path \""+filePath+"\", rowTag \"pages\", valueTag \"page\")");
       Dataset<Row> wikiDataSet = sparkSession.sql("SELECT * FROM wikidata limit 100");
       wikiDataSet.show(30);*/

    }

}
