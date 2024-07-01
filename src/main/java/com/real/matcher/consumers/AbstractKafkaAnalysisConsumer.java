package com.real.matcher.consumers;

import com.real.matcher.Matcher;
import com.real.matcher.MatcherImpl;
import com.real.matcher.producers.CSVProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

public abstract class AbstractKafkaAnalysisConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaAnalysisConsumer.class);

    private static KafkaConsumer<String, String> kafkaConsumer(String groupId) throws IOException {

        Properties kafkaConsumerProperties = new Properties();
        try (InputStream input = MatcherImpl.class.getResourceAsStream("/kafka-consumer.properties")) {
            kafkaConsumerProperties.load(input);
        }

        Properties props = new Properties();
        Set<Object> keySet = kafkaConsumerProperties.keySet();
        for (Object key : keySet) {
            props.put(key, kafkaConsumerProperties.getProperty((String) key));
        }
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new KafkaConsumer<>(props);
    }

    public List<Matcher.IdMapping> analyzeMessages(String topic, int partition, String groupId, String header,
                                                   Properties config)
            throws IOException {

        List<Matcher.IdMapping> idMappings = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = kafkaConsumer(groupId)) {
            Collection<TopicPartition> partitions = Collections.singletonList(new TopicPartition(topic, partition));
            consumer.assign(partitions);

            boolean done = false;
            long counter = 0;
            while (!done) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(
                        Long.parseLong(config.getProperty("kafka.consumer.poll.timeout"))));
                for (ConsumerRecord<String, String> consumerRecord : records) {
                    String csvData = consumerRecord.value();
                    if (csvData.equalsIgnoreCase(config.getProperty(CSVProducer.KAFKA_PRODUCER_END_SIGNAL))) {
                        logger.info("End signal observed: {}",
                                config.getProperty(CSVProducer.KAFKA_PRODUCER_END_SIGNAL));
                        done = true;
                    }
                    if (!done) {
                        analyzeCSVData(csvData, idMappings, header, config);
                    }
                    if (counter % 1000 == 0) {
                        logger.info("{}: {} consumed {} records...", Thread.currentThread().getId(), topic, counter);
                    }
                    counter++;

                }
                consumer.commitSync();
            }
        }
        return idMappings;
    }

    abstract void analyzeCSVData(String csvData, List<Matcher.IdMapping> list, String header,
                                 Properties props) throws IOException;


}
