package org.cloud.data.crud;

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

public class IcebergCrudExample {
    public static void main(String[] args) throws NoSuchTableException {
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

        spark.sql("CREATE OR REPLACE TABLE local.db.employees (" +
                "employee_id BIGINT, " +
                "name STRING, " +
                "department STRING, " +
                "salary DOUBLE, " +
                "hire_date STRING) " +
                "USING iceberg");

        spark.sql("INSERT INTO local.db.employees VALUES " +
                "(1, 'Alice', 'Engineering', 85000, '2020-01-15'), " +
                "(2, 'Bob', 'HR', 65000, '2019-03-22'), " +
                "(3, 'Charlie', 'Marketing', 70000, '2018-10-13')");
        spark.sql("SELECT * FROM local.db.employees").show();

        spark.sql("UPDATE local.db.employees " +
                "SET salary = salary * 1.10 " +
                "WHERE name = 'Charlie'");


        spark.sql("DELETE FROM local.db.employees WHERE name = 'Bob'");
        spark.sql("select * from local.db.employees.history").show();
        spark.sql("SELECT * FROM local.db.employees").show();


    // Query previous version using a specific timestamp
        spark.sql("SELECT * FROM local.db.employees VERSION AS OF 7837430298858110907").show();

    }
}
