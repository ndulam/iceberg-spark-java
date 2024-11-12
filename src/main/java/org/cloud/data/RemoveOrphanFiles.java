package org.cloud.data;
/*
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;

public class RemoveOrphanFiles {
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
        Configuration conf = new Configuration();
        String warehousePath = "file:///D:/sparksetup/iceberg/spark_warehouse/maintanence";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        TableIdentifier tableIdentifier = TableIdentifier.of( "db","table");
        catalog.listTables(Namespace.of("db")).get(0);

        Table table = catalog.loadTable(tableIdentifier);
        //System.out.println(table.newScan().snapshot().manifestListLocation());
        //SparkActions.get().expireSnapshots(table).expireFiles().show();
        //SparkActions.get().rewriteDataFiles(table).execute();


    }
}
*/