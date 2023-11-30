package org.cloud.data.alterTable;

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

public class InsertDataTable2 {
    public static void main(String[] args) throws NoSuchTableException {
        System.out.println("Hello World");
        System.setProperty("hadoop.home.dir", "D:\\sparksetup\\hadoop");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.local.type","hadoop")
                .set("spark.sql.catalog.local.warehouse", "file:///D:/sparksetup/iceberg/spark_warehouse/maintanence")
                ;
        SparkSession spark = SparkSession.builder().appName("Example Spark App").config(sparkConf).getOrCreate();

        StructType structType = new StructType();
        structType = structType.add("ts", DataTypes.LongType, false);
        structType = structType.add("uuid", DataTypes.StringType, false);
        structType = structType.add("rider", DataTypes.StringType, false);
        structType = structType.add("driver", DataTypes.StringType, false);
        structType = structType.add("fare", DataTypes.DoubleType, false);
        structType = structType.add("city", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create(1695159649087L,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"));
        nums.add(RowFactory.create(1695516137016L,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo" ));
        nums.add(RowFactory.create(1695115999911L,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",20.85,"chennai"));

        Dataset<Row> dataset = spark.createDataFrame(nums, structType);
        dataset.writeTo("local.uber.trips2").append();


    }
}
