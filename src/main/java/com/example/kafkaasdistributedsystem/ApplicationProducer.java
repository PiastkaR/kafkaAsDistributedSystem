package com.example.kafkaasdistributedsystem;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ApplicationProducer {
    private static final String TOPIC = "events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";//added in case one server is down the other becomes a gateway

//    public static void main(String[] args) {
//        Producer<Long, String> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);
//        try {
//            produceMessages(10, kafkaProducer);
//        } catch (ExecutionException | InterruptedException e) {
//            e.printStackTrace();
//        } finally {
//            kafkaProducer.flush();
//            kafkaProducer.close();
//        }
//    }

    public static void produceMessages(int numberOfMessages, Producer<Long, String> kafkaProducer) throws ExecutionException, InterruptedException {
        for (int i = 0; i < numberOfMessages; i++) {
            long key = i;
            String value = String.format("event %d", i);
            long timestamp = System.currentTimeMillis();
            ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, key, value);//hash is applied on hey best for decoupling
            /** if key is not used a global setup is performed by default Kafka uses ROUND ROBIN algorithm**/
            RecordMetadata recordMetadata = kafkaProducer.send(record).get();//tells where object has landed
            System.out.printf("Sent message with key %d to partition %d with offset %d%n", key, recordMetadata.partition(), recordMetadata.offset());
        }
    }

    public static Producer<Long, String> createKafkaProducer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "events-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());//any pojo might be key but we need to indicate serializer
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<Long, String>(properties);
    }

}
