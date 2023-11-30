package org.cloud.data.timetravel;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

public class CreateTable {
    public static void main(String[] args) throws NoSuchTableException {
        System.out.println("Hello World");
        System.setProperty("hadoop.home.dir", "D:\\sparksetup\\hadoop");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.local.type","hadoop")
                .set("spark.sql.catalog.local.warehouse", "file:///D:/sparksetup/iceberg/spark_warehouse/timetravel")
                ;
        SparkSession spark = SparkSession.builder().appName("Example Spark App").config(sparkConf).getOrCreate();
        spark.sql("CREATE OR REPLACE TABLE local.uber.trips (ts bigint, uuid string, rider string, driver string, fare float, city string) " +
                "USING iceberg PARTITIONED BY (city)");


    }
}
