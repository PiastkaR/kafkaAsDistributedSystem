package com.example.kafkaasdistributedsystem;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ApplicationConsumer {

    private static final String TOPIC = "events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

//    public static void main(String[] args) {
//        String consumerGroup = "defaultConsumerGroup";
//        if (args.length == 1) {
//            consumerGroup = args[0];//got consumer group name from cmd
//        }
//        System.out.println("Consumer is a part of consumer group: " + consumerGroup);
//        Consumer<Long, String> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
//        consumeMessages(TOPIC, kafkaConsumer);
//    }

    public static void consumeMessages(String topic, Consumer<Long, String> kafkaConsumer) {
        kafkaConsumer.subscribe(List.of(topic));
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));//waiting to consume records from the topic
            if (consumerRecords.isEmpty()) {//no records received
                System.out.println("No records found, waiting for new messages...");//we get multiple records due to optimization
            }
            for (ConsumerRecord<Long, String> record : consumerRecords) {
                System.out.println("Received record with key: " + record.key() + ", value: " + record.value() +
                        ", partition: " + record.partition() + ", offset: " + record.offset());
            }
            //do sth with records
            kafkaConsumer.commitAsync();//consumer successfully consumed messages
        }
    }

    public static Consumer<Long, String> createKafkaConsumer(String bootstrapServer, String consumerGroup) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); //best practice is to inform about consumed message manually

        return new KafkaConsumer<>(properties);
    }
}
