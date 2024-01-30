package nooboo.BiliStat.utils;

import org.apache.spark.sql.SparkSession;

public class SparkSessionUtil {

    public static SparkSession createSparkSession(String uriConst, String uri) {
        return SparkSession.builder()
                .appName("BiliStat")
                .master("local")
                .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
                .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
                .config(uriConst, uri)
                .getOrCreate();
    }
}
