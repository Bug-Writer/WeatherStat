package nooboo.BiliStat.hbasereader;

import nooboo.BiliStat.core.DataReader;
import nooboo.BiliStat.utils.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.*;

public class HBaseReader implements DataReader {

    public void readData() {
        Config config = ConfigFactory.load("settings.conf");
        String hbaseUri = config.getString("hbaseUri");
        SparkSession spark = SparkSessionUtil.createSparkSession(Constants.HBASE, hbaseUri);

        String hbaseTable = /*config.getString("hbaseTable")*/ "videoInfo";
        String catalog = "{...}";

        DataFrameReader dataFrameReader = spark.read().format("org.apache.hadoop.hbase.spark");
        Dataset<Row> df = dataFrameReader.option("hbase.table", hbaseTable)
                .option("hbase.catalog", catalog)
                .load();

        spark.stop();
    }
}
