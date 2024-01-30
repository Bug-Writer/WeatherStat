package nooboo.BiliStat.core;

import nooboo.BiliStat.core.utils.ClassUtil;

public class WorkFlowManager {

    public static void main(String[] args) {
        // ... 其他初始化代码 ...

        // 根据需要创建不同的DataReader和DataConsumer实例
        DataReader dataReader = ClassUtil.instantiate("nooboo.BiliStat.consumer.HBaseDataReader", DataReader.class);
        KafkaDataConsumer dataConsumer = ClassUtil.instantiate("nooboo.BiliStat.consumer.KafkaDataConsumer", KafkaDataConsumer.class);

        // 使用接口方法而不是具体类
        dataConsumer.consumeData();
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
