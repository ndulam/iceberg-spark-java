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
                .set("spark.sql.catalog.local.warehouse", "file:///D:/sparksetup/iceberg/spark_warehouse")
                ;
        SparkSession spark = SparkSession.builder().appName("Example Spark App").config(sparkConf).getOrCreate();
        spark.sql("CREATE OR REPLACE TABLE local.db.table (ts bigint, uuid string, rider string, driver string, fare float, city string) " +
                "USING iceberg PARTITIONED BY (city)");

        spark.sql("CREATE TABLE local.db.emp_partitioned_month( id INT,role STRING, department STRING, join_date DATE ) " +
                "USING iceberg PARTITIONED BY (months(join_date)) ");

    }
}
