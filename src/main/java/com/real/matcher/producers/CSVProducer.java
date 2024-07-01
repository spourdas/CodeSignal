package com.real.matcher.producers;

import com.real.matcher.MatcherImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CSVProducer {

    private static final Logger logger = LoggerFactory.getLogger(CSVProducer.class);
    public static final String KAFKA_PRODUCER_END_SIGNAL = "kafka.producer.end.signal";

    public void publishMessages(Stream<String> stringStream, String id, String topic, String groupId, Properties config)
            throws IOException {

        Properties kafkaProducerProperties = new Properties();

        InputStream input = MatcherImpl.class.getResourceAsStream("/kafka-producer.properties");

        kafkaProducerProperties.load(input);

        Properties properties = new Properties();

        Set<Object> keySet = kafkaProducerProperties.keySet();
        for (Object key : keySet) {
            properties.put(key, kafkaProducerProperties.getProperty((String) key));
        }

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, id);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        Producer<String, String> csvProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        try {

            stringStream.forEach((line -> {

                ProducerRecord<String, String> csvRecord = new ProducerRecord<>(topic,
                        UUID.randomUUID().toString(), line);
                csvProducer.send(csvRecord, (metadata, exception) -> {
                    if (metadata == null) {
                        logger.info("Error Sending CSV Record -> {}", csvRecord.value());
                    }
                });

            }));

            int numPartitions = Integer.parseInt(config.getProperty("kafka.number.of.partitions"));

            IntStream.range(1, 2).forEach(i -> IntStream.range(0, numPartitions).forEach(partition -> {
                ProducerRecord<String, String> endRecord = new ProducerRecord<>(topic, partition,
                        UUID.randomUUID().toString(), config.get(KAFKA_PRODUCER_END_SIGNAL).toString());
                csvProducer.send(endRecord, (metadata, exception) -> {
                    if (metadata == null) {
                        logger.info("Error Sending CSV Record -> {}", endRecord.value());
                    }
                });
            }));
        } finally {
            csvProducer.flush();
            csvProducer.close();
        }

    }

}
