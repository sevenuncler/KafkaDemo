import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerThread extends Thread {
    private KafkaConsumer consumer;
    private String group;

    ConsumerThread() {
        final String topic = "test";
        final String group = "consumer-2";
        final String url = "localhost:9092";

        HashMap<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Collections.singletonList(topic));
        this.consumer = consumer;
    }

    @Override
    public void run() {

        while (true) {
            ConsumerRecords<String, String> poll = this.consumer.poll(500);
            poll.forEach(
                    record -> System.out.println(String.format("topic: %s, group:%s key: %s, value: %s, offset:%d.",
                            record.topic(), this, record.key(), record.value(), record.offset())));
            // 提交偏移量.
            consumer.commitSync();
        }
    }
}