package org.cloud.data;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class ReadAvroSpark {
    public static void main(String[] args) throws NoSuchTableException {
        System.out.println("Hello World");
        System.setProperty("hadoop.home.dir", "D:\\sparksetup\\hadoop");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.spark_catalog.type","hive")
                .set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.local.type","hadoop")
                .set("spark.sql.catalog.local.warehouse", "file:///D:/sparksetup/iceberg/spark_warehouse")
                ;
        SparkSession spark = SparkSession.builder().appName("Example Spark App").config(sparkConf).getOrCreate();
        //spark.read().format("avro").load("file:///D:/sparksetup/iceberg/spark_warehouse/db/table/metadata/snap-6428656557406270510-1-8ea020ad-27b1-4f7f-931a-3a3dbf60a184.avro").show(false);
        //spark.read().format("avro").load("file:///D:/sparksetup/iceberg/spark_warehouse/db/table/metadata/8ea020ad-27b1-4f7f-931a-3a3dbf60a184-m1.avro").show(false);
        //spark.read().format("avro").load("file:///D:/sparksetup/iceberg/spark_warehouse/db/table/metadata/8ea020ad-27b1-4f7f-931a-3a3dbf60a184-m0.avro").show(false);

        spark.read().parquet("file:///D:/sparksetup/iceberg/spark_warehouse/db/table/data/city=chennai/*.parquet").show();




    }
}
