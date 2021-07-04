package com.dr4m.kafkademo;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author liuwei
 * @date 2021/6/15 20:22
 */
public class ProducerFastStart {

    public static final String BROKER_LIST = "localhost:9092";
    public static final String TOPIC = "topic-demo";

    public static void main(String[] args) {
        Properties properties = initConfig();

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello, kafka!");

        Future<RecordMetadata> future = producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.out.println("error");
                } else {
                    System.out.println(metadata.topic() + "_" + metadata.partition() + "_" + metadata.offset());
                }
            }
        });

        producer.close();
    }

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", BROKER_LIST);
        properties.put("client.id", "producer.client.id.demo");
        return properties;
    }
}
