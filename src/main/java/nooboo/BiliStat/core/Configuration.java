package nooboo.BiliStat.core;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {

    private Properties properties;

    public Configuration() {
        this.properties = new Properties();

        // 从config.properties文件加载
        try (InputStream input = new FileInputStream("config.properties")) {
            properties.load(input);
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public String getDatastoreConsumerClassName() {
        String type = properties.getProperty("database.type");
        switch (type) {
            case "hbase":
                return "nooboo.BiliStat.consumer.HBaseKafkaConsumer";
            case "mongodb":
                return "nooboo.BiliStat.consumer.MongoDBKafkaConsumer";
            default:
                throw new IllegalStateException("Unsupported database type: " + type);
        }
    }

    public String getDatastoreReaderClassName() {
        String type = properties.getProperty("database.type");
        switch (type) {
            case "hbase":
                return "nooboo.BiliStat.consumer.HBaseDataReader";
            case "mongodb":
                return "nooboo.BiliStat.consumer.MongoDBDataReader";
            default:
                throw new IllegalStateException("Unsupported database type: " + type);
        }
    }

    // 其他获取配置的方法...
}

