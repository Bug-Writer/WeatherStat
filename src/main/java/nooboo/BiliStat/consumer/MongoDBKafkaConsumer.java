package nooboo.BiliStat.consumer;

import nooboo.BiliStat.core.KafkaDataConsumer;
import nooboo.BiliStat.utils.KafkaConsumerUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class MongoDBKafkaConsumer implements KafkaDataConsumer {

    public void consumeData() {
        Consumer<String, String> consumer = KafkaConsumerUtil.createConsumer("videoInfo");

        final int giveUp = 100;   // Maximum attempts without message
        int noRecordsCount = 0;

        while (true) {
            /*final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                ++noRecordsCount;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
                // TODO: Write record.value() to HBase
            });*/

            consumer.commitAsync();
        }
        /*
        consumer.close();
        System.out.println("DONE");
        */
    }
}
