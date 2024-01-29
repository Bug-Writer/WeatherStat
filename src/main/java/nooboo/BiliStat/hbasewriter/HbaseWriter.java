package nooboo.BiliStat.hbasewriter;

import nooboo.BiliStat.utils.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.*;

public class HbaseWriter {

    public static void main(String[] args) {
        Config config = ConfigFactory.load("settings.conf");
        String hbaseUri = config.getString("hbaseUri");
        SparkSession spark = SparkSessionUtils.createSparkSession(Constants.HBASE, hbaseUri);

        String hbaseTable = /*config.getString("hbaseTable")*/ "videoInfo";
        String catalog = "{...}";

        DataFrameReader dataFrameReader = spark.read().format("org.apache.hadoop.hbase.spark");
        Dataset<Row> df = dataFrameReader.option("hbase.table", hbaseTable)
                .option("hbase.catalog", catalog)
                .load();

        spark.stop();
    }
}
