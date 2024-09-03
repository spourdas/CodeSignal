package com.real.matcher.utils;

import org.apache.kafka.clients.admin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.Normalizer;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.lang.Integer.parseInt;

public class Utils {
    public static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public Utils() {
    }

    public static String standardizeName(String name) {
        if (name == null) {
            return name;
        }
        String standardized = name.trim().toLowerCase(Locale.ROOT);
        standardized = Normalizer.normalize(standardized, Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "");
        standardized = standardized.replaceAll("[^a-zA-Z0-9\\s]", "")
                .replaceAll("\\s+", " ");
        return standardized;
    }

    public static int convertStringToInt(String string) {
        int num = 0;
        try {
            num = parseInt(string);
        } catch (Exception e) {
            logger.info("");
        }
        return num;
    }

    public static int calculateLevenshteinThreshold(String string1, String string2, int acceptancePercentage) {
        int minLength = Math.min(string1.length(), string2.length());
        return (int) Math.round(((float) minLength * (float) acceptancePercentage) / 100.0);
    }

    public static void createKafkaTopicIfNotExists(String bootstrapServers, String topicName, int numPartitions)
            throws ExecutionException, InterruptedException {

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = KafkaAdminClient.create(adminProps)) {
            ListTopicsOptions options = new ListTopicsOptions().timeoutMs(10000);
            Set<String> existingTopics = adminClient.listTopics(options).names().get();
            if (!existingTopics.contains(topicName)) {
                NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
                CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
                createTopicsResult.all().get();
                logger.info("Topic {} created successfully.", topicName);
            }
        }
    }

}
