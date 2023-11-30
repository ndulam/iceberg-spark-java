package org.cloud.data.maintanence;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

public class AlterTablePartition3_5 {
    public static void main(String[] args) throws NoSuchTableException {
        System.out.println("Hello World");
        System.setProperty("hadoop.home.dir", "D:\\sparksetup\\hadoop");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.local.type","hadoop")
                .set("spark.sql.catalog.local.warehouse", "file:///D:/sparksetup/iceberg/spark_warehouse/maintanence");

        SparkSession spark = SparkSession.builder().appName("Example Spark App").config(sparkConf).getOrCreate();

        //spark.sql("ALTER TABLE local.uber.trips DROP PARTITION FIELD city");
        spark.sql("ALTER TABLE local.uber.trips SET TBLPROPERTIES ('write.metadata.previous-versions-max'='1')");
        spark.sql("ALTER TABLE local.uber.trips SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled'='true')");
        spark.sql("ALTER TABLE local.uber.trips RENAME COLUMN city to city2");


        //spark.sql("ALTER TABLE local.uber.trips ADD PARTITION FIELD driver");
        //spark.sql("ALTER TABLE local.uber.trips DROP PARTITION FIELD driver");

    }
}
