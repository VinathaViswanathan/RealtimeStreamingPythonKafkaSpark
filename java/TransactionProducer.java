package com.kafkasparkarch.kafkaproducer;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TransactionProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092";
    private static final String TOPIC = "financial_transactions";

    private static final int NUM_PARTITIONS = 5;
    private static final short REPLICATION_FACTOR = 3;

    public static void main(String[] args) throws Exception {
        createTopic();

        for (int i = 0; i < 3; i++) {
            Thread thread = new Thread(() -> produce());
            thread.start();
        }
    }

    private static void createTopic() throws ExecutionException, InterruptedException {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        try (AdminClient admin = AdminClient.create(config)) {
            Set<String> existing = admin.listTopics().names().get();
            if (!existing.contains(TOPIC)) {
                NewTopic newTopic = new NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
                admin.createTopics(Collections.singletonList(newTopic)).all().get();
                System.out.println("Topic created.");
            } else {
                System.out.println("Topic already exists.");
            }
        }
    }

    private static void produce() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1000);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();

        while (true) {
            String transaction = generateTransaction(random);
            String key = "user_" + random.nextInt(100);

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, transaction);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Delivery failed: " + exception.getMessage());
                } else {
                    System.out.printf("Produced: %s\n", transaction);
                }
            });

        }
    }

    private static String generateTransaction(Random random) {
        String uuid = UUID.randomUUID().toString();
        String userId = "user_" + random.nextInt(100);
        double amount = 50000 + (100000 * random.nextDouble());
        long time = System.currentTimeMillis() / 1000;
        String merchant = "merchant_" + (1 + random.nextInt(3));
        String type = random.nextBoolean() ? "purchase" : "refund";
        String location = "location_" + (1 + random.nextInt(50));
        String payment = List.of("credit_card", "paypal", "bank_transfer").get(random.nextInt(3));
        boolean international = random.nextBoolean();
        String currency = List.of("USD", "EUR", "GBP").get(random.nextInt(3));

        return String.format("{\"transactionId\":\"%s\",\"userId\":\"%s\",\"amount\":%.2f,\"transactionTime\":%d,"
                + "\"merchantId\":\"%s\",\"transactionType\":\"%s\",\"location\":\"%s\",\"paymentMethod\":\"%s\","
                + "\"isInternational\":\"%s\",\"currency\":\"%s\"}",
                uuid, userId, amount, time, merchant, type, location, payment, international, currency);
    }
}
