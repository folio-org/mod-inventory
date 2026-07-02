package org.folio.inventory;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaTopicNameHelper;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static java.time.Duration.ofMinutes;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public final class KafkaUtility {
  private static final Logger logger = LogManager.getLogger();
  public static final String KAFKA_ENV_VALUE = "env";
  public static final int MAX_REQUEST_SIZE = 1048576;
  private static final long DEFAULT_CHECK_TIMEOUT_MS = 3000L;
  private static final long ASSIGNMENT_POLL_INTERVAL_MS = 100L;
  private static final ConcurrentMap<String, ConcurrentMap<TopicPartition, Long>> TOPIC_NEXT_OFFSETS = new ConcurrentHashMap<>();

  public static final DockerImageName IMAGE_NAME
    = DockerImageName.parse("apache/kafka-native:3.8.0");

  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(IMAGE_NAME)
      .withStartupAttempts(3);

  private KafkaUtility() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  public static void startKafka() {
    TOPIC_NEXT_OFFSETS.clear();
    KAFKA_CONTAINER.start();

    logger.info("starting Kafka host={} port={}",
      KAFKA_CONTAINER.getHost(), KAFKA_CONTAINER.getFirstMappedPort());

    var kafkaHost = KAFKA_CONTAINER.getHost();
    var kafkaPort = String.valueOf(KAFKA_CONTAINER.getFirstMappedPort());
    logger.info("Starting Kafka host={} port={}", kafkaHost, kafkaPort);
    System.setProperty("kafka-port", kafkaPort);
    System.setProperty("kafka-host", kafkaHost);

    await().atMost(ofMinutes(1)).until(KAFKA_CONTAINER::isRunning);

    logger.info("finished starting Kafka");
  }

  public static void stopKafka() {
    TOPIC_NEXT_OFFSETS.clear();
    if (KAFKA_CONTAINER.isRunning()) {
      logger.info("stopping Kafka host={} port={}",
        KAFKA_CONTAINER.getHost(), KAFKA_CONTAINER.getFirstMappedPort());

      KAFKA_CONTAINER.stop();
      logger.info("finished stopping Kafka");
    } else {
      logger.info("Kafka container already stopped");
    }
  }

  public static List<ConsumerRecord<String, String>> checkKafkaEventSent(String tenant, String eventType) {
    return checkKafkaEventSent(tenant, eventType, DEFAULT_CHECK_TIMEOUT_MS);
  }

  public static List<ConsumerRecord<String, String>> checkKafkaEventSent(String tenant, String eventType, long timeout) {
    Properties consumerProperties = getConsumerProperties();
    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties)) {
      String topicName = formatToKafkaTopicName(tenant, eventType);
      kafkaConsumer.subscribe(Collections.singletonList(topicName));

      if (!waitForAssignment(kafkaConsumer, timeout)) {
        return List.of();
      }

      seekToNextOffsets(kafkaConsumer, topicName);

      ConsumerRecords<String, String> records = pollUntilAnyRecords(kafkaConsumer, timeout);
      if (records.isEmpty()) {
        return List.of();
      }

      rememberNextOffsets(topicName, records);
      List<ConsumerRecord<String, String>> result = new ArrayList<>();
      records.forEach(result::add);
      return result;
    }
  }

  public static RecordMetadata sendEvent(Map<String, String> kafkaHeaders, String tenantId,
                                  String topic, String key, String value) throws ExecutionException, InterruptedException {
    var producerProperties = getProducerProperties();
    try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties)) {
      var topicName = formatToKafkaTopicName(tenantId, topic);
      var producerRecord = new ProducerRecord<>(topicName, key, value);
      kafkaHeaders.forEach((k, v) -> {
        if (v != null) {
          producerRecord.headers().add(k, v.getBytes());
        }
      });

      return kafkaProducer.send(producerRecord).get();
    }
  }

  public static String[] getKafkaHostAndPort() {
    return KAFKA_CONTAINER.getBootstrapServers().split(":");
  }


  private static Properties getConsumerProperties() {
    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30 seconds
    consumerProperties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 10 seconds
    consumerProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
    return consumerProperties;
  }

  private static boolean waitForAssignment(KafkaConsumer<String, String> kafkaConsumer, long timeoutMs) {
    long deadline = System.nanoTime() + Duration.ofMillis(timeoutMs).toNanos();
    while (kafkaConsumer.assignment().isEmpty() && System.nanoTime() < deadline) {
      kafkaConsumer.poll(Duration.ofMillis(ASSIGNMENT_POLL_INTERVAL_MS));
    }
    return !kafkaConsumer.assignment().isEmpty();
  }

  private static void seekToNextOffsets(KafkaConsumer<String, String> kafkaConsumer, String topicName) {
    ConcurrentMap<TopicPartition, Long> nextOffsets = TOPIC_NEXT_OFFSETS.computeIfAbsent(topicName,
      ignored -> new ConcurrentHashMap<>());
    for (TopicPartition topicPartition : kafkaConsumer.assignment()) {
      Long nextOffset = nextOffsets.get(topicPartition);
      if (nextOffset == null) {
        kafkaConsumer.seekToBeginning(Collections.singleton(topicPartition));
      } else {
        kafkaConsumer.seek(topicPartition, nextOffset);
      }
    }
  }

  private static ConsumerRecords<String, String> pollUntilAnyRecords(KafkaConsumer<String, String> kafkaConsumer,
                                                                      long timeoutMs) {
    long deadline = System.nanoTime() + Duration.ofMillis(timeoutMs).toNanos();
    while (System.nanoTime() < deadline) {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(ASSIGNMENT_POLL_INTERVAL_MS));
      if (!records.isEmpty()) {
        return records;
      }
    }
    return ConsumerRecords.empty();
  }

  private static void rememberNextOffsets(String topicName, ConsumerRecords<String, String> records) {
    ConcurrentMap<TopicPartition, Long> nextOffsets = TOPIC_NEXT_OFFSETS.computeIfAbsent(topicName,
      ignored -> new ConcurrentHashMap<>());
    for (TopicPartition topicPartition : records.partitions()) {
      List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
      ConsumerRecord<String, String> lastRecord = partitionRecords.get(partitionRecords.size() - 1);
      nextOffsets.put(topicPartition, lastRecord.offset() + 1);
    }
  }

  private static Properties getProducerProperties() {
    Properties producerProperties = new Properties();
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return producerProperties;
  }

  private static String formatToKafkaTopicName(String tenant, String eventType) {
    return KafkaTopicNameHelper.formatTopicName(KAFKA_ENV_VALUE, getDefaultNameSpace(), tenant, eventType);
  }
}
