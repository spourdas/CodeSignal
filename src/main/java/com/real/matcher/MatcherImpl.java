package com.real.matcher;

import com.real.matcher.cache.EhcacheManager;
import com.real.matcher.consumers.ActorsDirectorsConsumer;
import com.real.matcher.consumers.FeedConsumer;
import com.real.matcher.consumers.MoviesConsumer;
import com.real.matcher.producers.CSVProducer;
import com.real.matcher.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class MatcherImpl implements Matcher {

    private static final Logger logger = LoggerFactory.getLogger(MatcherImpl.class);

    public static final String KAFKA_MOVIES_ID = "KafkaMoviesProducer";
    public static final String KAFKA_MOVIES_GROUP = "movies-group";
    public static final String KAFKA_MOVIES_TOPIC = "movies";
    public static final String KAFKA_ACTORS_ID = "KafkaActorsDirectorsProducer";
    public static final String KAFKA_ACTORS_GROUP = "actors-group";
    public static final String KAFKA_ACTORS_TOPIC = "actors";
    public static final String KAFKA_BOOTSTRAP_SERVER = "bootstrap.servers";

    public MatcherImpl(CsvStream movieDb, CsvStream actorAndDirectorDb) throws IOException {


        Properties config = new Properties();
        InputStream input = MatcherImpl.class.getResourceAsStream("/config.properties");
        config.load(input);

        Properties kafkaProps = new Properties();
        input = MatcherImpl.class.getResourceAsStream("/kafka-producer.properties");
        kafkaProps.load(input);

        int numPartitions = Integer.parseInt(config.getProperty("kafka.number.of.partitions"));

        try {
            Utils.createKafkaTopicIfNotExists(kafkaProps.getProperty(KAFKA_BOOTSTRAP_SERVER), KAFKA_MOVIES_TOPIC,
                    numPartitions);
        } catch (ExecutionException e) {
            throw new MatcherException("Unable to create topic " + KAFKA_MOVIES_TOPIC, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MatcherException("Unable to create topic " + KAFKA_MOVIES_TOPIC, e);
        }

        CountDownLatch latch = new CountDownLatch(2 * numPartitions);

        logger.info("Starting to consume movies....");
        ExecutorService executor = Executors.newFixedThreadPool(numPartitions);

        for (int partition = 0; partition < numPartitions; partition++) {
            int finalPartition = partition;
            executor.submit(
                    () -> {
                        MoviesConsumer moviesConsumer = new MoviesConsumer();
                        try {
                            moviesConsumer.storeMessages(KAFKA_MOVIES_TOPIC, finalPartition, KAFKA_MOVIES_GROUP,
                                    config);
                            latch.countDown();
                        } catch (IOException e) {
                            throw new MatcherException("Unable to store " + KAFKA_MOVIES_TOPIC + " messages", e);
                        }
                    });

        }

        logger.info("Finished consuming movies....");

        logger.info("Streaming movies....");
        CSVProducer moviesProducer = new CSVProducer();
        moviesProducer.publishMessages(movieDb.getDataRows(), KAFKA_MOVIES_ID, KAFKA_MOVIES_TOPIC,
                KAFKA_MOVIES_GROUP, config);
        logger.info("Finished Streaming movies....");

        try {
            Utils.createKafkaTopicIfNotExists(kafkaProps.getProperty(KAFKA_BOOTSTRAP_SERVER), KAFKA_ACTORS_TOPIC,
                    numPartitions);
        } catch (ExecutionException e) {
            throw new MatcherException("Unable to create topic " + KAFKA_ACTORS_TOPIC, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MatcherException("Unable to create topic " + KAFKA_ACTORS_TOPIC, e);
        }

        logger.info("Starting to consume actors and directors....");
        executor = Executors.newFixedThreadPool(numPartitions);

        for (int partition = 0; partition < numPartitions; partition++) {
            int finalPartition = partition;
            executor.submit(() -> {
                try {
                    ActorsDirectorsConsumer actorsDirectorsConsumer = new ActorsDirectorsConsumer();
                    actorsDirectorsConsumer.storeMessages(KAFKA_ACTORS_TOPIC, finalPartition,
                            KAFKA_ACTORS_GROUP, config);
                    latch.countDown();
                } catch (IOException e) {
                    throw new MatcherException("Unable to store " + KAFKA_ACTORS_TOPIC + " messages", e);
                }

            });
        }

        logger.info("Finished consuming actors and directors....");

        logger.info("Streaming actors and directors....");
        CSVProducer actorsDirectorsProducer = new CSVProducer();
        actorsDirectorsProducer.publishMessages(actorAndDirectorDb.getDataRows(), KAFKA_ACTORS_ID,
                KAFKA_ACTORS_TOPIC, KAFKA_ACTORS_GROUP, config);
        logger.info("Finished Streaming actors and directors....");

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MatcherException("Unable to wait for process latches", e);
        }


    }

    @Override
    public List<IdMapping> match(DatabaseType databaseType, CsvStream externalDb) {

        try {

            List<IdMapping> idMappingList = new ArrayList<>(List.of());


            Properties feedProps = new Properties();
            String feedConfigName = "/" + databaseType.toString().toLowerCase() + ".properties";
            InputStream input = MatcherImpl.class.getResourceAsStream(feedConfigName);
            try {
                feedProps.load(input);
            } catch (IOException e) {
                throw new MatcherException("Unable to load " + feedConfigName, e);
            }

            String kafkaConfigName = "/kafka-producer.properties";
            Properties kafkaProps = new Properties();
            input = MatcherImpl.class.getResourceAsStream(kafkaConfigName);
            try {
                kafkaProps.load(input);
            } catch (IOException e) {
                throw new MatcherException("Unable to load " + kafkaConfigName, e);
            }

            String kafkaTopicName = feedProps.getProperty("kafka.topic.name");
            String kafkaGroupName = feedProps.getProperty("kafka.group.name");
            String kafkaStreamId = feedProps.getProperty("kafka.producer.id");

            int numPartitions = Integer.parseInt(feedProps.getProperty("kafka.number.of.partitions"));
            try {
                Utils.createKafkaTopicIfNotExists(kafkaProps.getProperty(KAFKA_BOOTSTRAP_SERVER), kafkaTopicName,
                        numPartitions);
            } catch (ExecutionException e) {
                throw new MatcherException("Unable to create Kafka topic " + kafkaTopicName, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MatcherException("Unable to create Kafka topic " + kafkaTopicName, e);
            }

            logger.info("Streaming {}....", kafkaTopicName);
            CSVProducer kafkaProducer = new CSVProducer();
            try {
                kafkaProducer.publishMessages(externalDb.getDataRows(), kafkaStreamId, kafkaTopicName,
                        kafkaGroupName, feedProps);
            } catch (IOException e) {
                throw new MatcherException("Unable to publish message for  Kafka topic " + kafkaTopicName, e);
            }
            logger.info("Finished Streaming {}....", kafkaTopicName);

            logger.info("Starting to consume {}....", kafkaTopicName);
            ExecutorService executor = Executors.newFixedThreadPool(Integer.parseInt(String.valueOf(numPartitions)));
            List<Callable<List<IdMapping>>> callables = new ArrayList<>();
            IntStream.range(0, numPartitions).forEachOrdered(partition -> {
                Callable<List<IdMapping>> callable = () -> {
                    FeedConsumer kafkaConsumer = new FeedConsumer();
                    return kafkaConsumer.analyzeMessages(kafkaTopicName, partition, kafkaGroupName,
                            externalDb.getHeaderRow(), feedProps);
                };
                callables.add(callable);
            });

            List<Future<List<IdMapping>>> futures = new ArrayList<>();
            for (Callable<List<IdMapping>> callable : callables) {
                futures.add(executor.submit(callable));
            }

            for (Future<List<IdMapping>> future : futures) {
                List<IdMapping> newList = null;
                try {
                    newList = future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new MatcherException("Unable to get results from thread for topic " + kafkaTopicName, e);
                } catch (ExecutionException e) {
                    throw new MatcherException("Unable to get results from thread for topic " + kafkaTopicName, e);
                }
                idMappingList.addAll(newList);
            }

            logger.info("Finished consuming {} ....", kafkaTopicName);

            executor.shutdown();

            return idMappingList;
        } finally {
            EhcacheManager.shutdownCacheManager();
        }

    }
}