package com.dexlock.spark.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.collection.mutable.WrappedArray;
import scala.collection.mutable.WrappedArray$;
import scala.xml.XML$;

import java.io.File;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.Struct;
import java.sql.Wrapper;
import java.util.ArrayList;

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
        //wikiDataSet1.printSchema();
        
        /*
        root
                |-- page: array (nullable = true)
                |    |-- element: struct (containsNull = true)
                |    |    |-- _VALUE: string (nullable = true)
                |    |    |-- _id: long (nullable = true)
                |    |    |-- _ns-id: long (nullable = true)
                |    |    |-- _ns-name: string (nullable = true)
                |    |    |-- _revision: long (nullable = true)
                |    |    |-- _title: string (nullable = true)
                |    |    |-- _type: string (nullable = true)*/

        JavaRDD expandedWikiRDD = wikiDataSet1.javaRDD().map(row->{
            return row.get(0);
        });
        System.out.println("****"+""+wikiDataSet1.count());
        System.out.println("###"+ expandedWikiRDD.count());
        StructType customSchema = new StructType(new StructField[] {
                new StructField("_VALUE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("_id", DataTypes.LongType, true, Metadata.empty()),
                new StructField("_ns-id", DataTypes.LongType, true, Metadata.empty()),
                new StructField("_ns-name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("_revision", DataTypes.LongType, true, Metadata.empty()),
                new StructField("_title", DataTypes.StringType, true, Metadata.empty()),
                new StructField("_type", DataTypes.StringType, true, Metadata.empty())
        });
        Encoder<Row> rowEncoder = Encoders.javaSerialization(Row.class);
        Dataset<Row> rowDataset1 = wikiDataSet1.map((MapFunction<Row, Row>) row -> {
           ArrayList<Struct> rowData = row.getAs(0);
           //ResultSet rs = rowData.getResultSet();
      /*     ArrayList<String> titles = new ArrayList<>();
           while (rs.next()){
               titles.add(rs.getString("_title"));
           }*/
           return RowFactory.create(rowData);
                }, rowEncoder);

        rowDataset1.printSchema();

       // wikiDataSet1.show();
  /*     sparkSession.sql("CREATE TABLE wikidata (_id string, _title string, _revision string, _type string, _page string, _nsid string, _nsname string, description string)\n" +
               "USING com.databricks.spark.xml\n" +
               "OPTIONS (path \""+filePath+"\", rowTag \"pages\", valueTag \"page\")");
       Dataset<Row> wikiDataSet = sparkSession.sql("SELECT * FROM wikidata limit 100");
       wikiDataSet.show(30);*/

/*        JavaRDD<WrappedArray<Struct>> rowJavaRDD = wikiDataSet1.javaRDD().flatMap(row -> {
            WrappedArray pages = (WrappedArray) row.get(0);
            //System.out.println("aaa"+pages.length());
            //System.out.println("aaaaa"+pages.size());
            //System.out.println("aaaaa"+pages.length());
           // System.out.println("aaaaaaa"+pages.clone().length());
           return RowFactory.create(pages);
        });*/

 /*       JavaRDD rowJavaRDD = wikiDataSet1.javaRDD().flatMap(row ->
                {
                    ArrayList<Struct> araayData = (ArrayList<Struct>) row.getAs(0);
                    return araayData;
                }
        );*/
        System.out.println("hhhhhhhhhhhhhhhhhhhhhh\n");
        //System.out.println(rowJavaRDD.count()+"\n");
        //System.out.println(rowJavaRDD.collect()+"\n");
        //Dataset wordDF = sparkSession.sqlContext().createDataFrame(rowJavaRDD, customSchema);
        //System.out.println("            oooooooooooo"+wordDF.count());;
    }

}
