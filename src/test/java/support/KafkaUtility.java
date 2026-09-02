package support;

import static org.folio.inventory.EntityLinksKafkaTopic.LINKS_STATS;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
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
import org.folio.kafka.KafkaTopicNameHelper;

/**
 * Mod-inventory specific Kafka producer/consumer helpers used by the Kafka integration tests.
 *
 * <p>The shared broker lifecycle is owned by
 * {@link org.folio.dataimport.testsupport.kafka.KafkaExtension} (registered in {@link KafkaTest});
 * this class only holds the module-specific event send/check logic and reads the running broker
 * coordinates through {@link #setBootstrapServers(String)}.
 */
public final class KafkaUtility {
  public static final String KAFKA_ENV_VALUE = "env";
  public static final int MAX_REQUEST_SIZE = 1048576;

  private static String bootstrapServers;

  private KafkaUtility() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  public static String getBootstrapServers() {
    return bootstrapServers;
  }

  public static void setBootstrapServers(String servers) {
    bootstrapServers = servers;
  }

  public static List<ConsumerRecord<String, String>> checkKafkaEventSent(String tenant, String eventType) {
    return checkKafkaEventSent(tenant, eventType, 3000);
  }

  public static List<ConsumerRecord<String, String>> checkKafkaEventSent(String tenant, String eventType,
                                                                         long timeout) {
    Properties consumerProperties = getConsumerProperties();
    ConsumerRecords<String, String> records;
    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties)) {
      if (LINKS_STATS.topicName().equals(eventType)) {
        kafkaConsumer.subscribe(Collections.singletonList(
          String.format("folio.%s.%s.%s", tenant, LINKS_STATS.moduleName(), LINKS_STATS.topicName())));
      } else {
        kafkaConsumer.subscribe(Collections.singletonList(formatToKafkaTopicName(tenant, eventType)));
      }
      records = kafkaConsumer.poll(Duration.ofMillis(timeout));
    }
    return IteratorUtils.toList(records.iterator()).stream().toList();
  }

  @SneakyThrows
  public static RecordMetadata sendEvent(Map<String, String> kafkaHeaders, String tenantId,
                                         String topic, String key, String value) {
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
    return bootstrapServers.split(":");
  }

  private static Properties getConsumerProperties() {
    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return consumerProperties;
  }

  private static Properties getProducerProperties() {
    Properties producerProperties = new Properties();
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return producerProperties;
  }

  private static String formatToKafkaTopicName(String tenant, String eventType) {
    return KafkaTopicNameHelper.formatTopicName(KAFKA_ENV_VALUE, getDefaultNameSpace(), tenant, eventType);
  }
}
