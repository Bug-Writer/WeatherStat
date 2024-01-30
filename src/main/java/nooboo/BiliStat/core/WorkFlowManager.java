package nooboo.BiliStat.core;

import nooboo.BiliStat.core.utils.ClassUtil;

public class WorkFlowManager {

    public static void main(String[] args) {
        // ... 其他初始化代码 ...

        // 根据需要创建不同的DataReader和DataConsumer实例
        KafkaDataConsumer kafkaDataConsumer = ClassUtil.instantiate(new Configuration().getDatastoreConsumerClassName(), KafkaDataConsumer.class);
        DataReader dataReader = ClassUtil.instantiate(new Configuration().getDatastoreReaderClassName(), DataReader.class);

        // 使用接口方法而不是具体类
        kafkaDataConsumer.consumeData();
        dataReader.readData();

        // ... 后续处理 ...

        // 关闭资源
        closeAllResources();
    }

    private static void closeAllResources() {
        // 关闭Kafka连接
        // 关闭数据库连接
        // 关闭Spark会话
        // 其他清理工作
    }
}
