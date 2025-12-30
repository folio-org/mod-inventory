package org.folio.inventory;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaTopicNameHelper;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.time.Duration.ofMinutes;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public final class KafkaUtility {
  private static final Logger logger = LogManager.getLogger();
  public static final String KAFKA_ENV_VALUE = "env";
  public static final int MAX_REQUEST_SIZE = 1048576;

  public static final DockerImageName IMAGE_NAME
    = DockerImageName.parse("apache/kafka-native:3.8.0");

  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(IMAGE_NAME)
      .withStartupAttempts(3);

  private KafkaUtility() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  public static void startKafka() {
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
    return checkKafkaEventSent(tenant, eventType, 3000);
  }

  public static List<ConsumerRecord<String, String>> checkKafkaEventSent(String tenant, String eventType, long timeout) {
    Properties consumerProperties = getConsumerProperties();
    ConsumerRecords<String, String> records;
    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties)) {
      kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

      kafkaConsumer.subscribe(Collections.singletonList(formatToKafkaTopicName(tenant, eventType)));
      records = kafkaConsumer.poll(Duration.ofMillis(timeout));
    }
    return IteratorUtils.toList(records.iterator()).stream().toList();
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
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return consumerProperties;
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
