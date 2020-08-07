package org.folio.inventory.kafka;

import io.vertx.core.json.JsonObject;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

@Getter
@Builder
@ToString
public class KafkaConfig {
  public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET = "kafka.consumer.auto.offset.reset";
  public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_DEFAULT = "earliest";

  public static final String KAFKA_CONSUMER_METADATA_MAX_AGE = "kafka.consumer.metadata.max.age.ms";
  public static final String KAFKA_CONSUMER_METADATA_MAX_AGE_DEFAULT = "30000";

  //  public static final String KAFKA_NUMBER_OF_PARTITIONS = "kafka.number.of.patitions";
  public static final String KAFKA_NUMBER_OF_PARTITIONS = "NUMBER_OF_PARTITIONS";
  public static final String KAFKA_NUMBER_OF_PARTITIONS_DEFAULT = "10";

  private String kafkaHost;
  private String kafkaPort;
  private String okapiUrl;
  private int replicationFactor;
  private String envId;

  public KafkaConfig() {
  }

  public KafkaConfig(String kafkaHost, String kafkaPort, String okapiUrl, int replicationFactor, String envId) {
    this.kafkaHost = kafkaHost;
    this.kafkaPort = kafkaPort;
    this.okapiUrl = okapiUrl;
    this.replicationFactor = replicationFactor;
    this.envId = envId;
  }

  public KafkaConfig(JsonObject jsonObject) {
    this.kafkaHost = jsonObject.getString("FOLIO_KAFKA_HOST", "kafka");
    this.kafkaPort = jsonObject.getString("FOLIO_KAFKA_PORT", "9092");
    this.okapiUrl = jsonObject.getString("OKAPI_URL", "http://okapi:9130");
    this.replicationFactor = jsonObject.getInteger("FOLIO_KAFKA_REPLICATION_FACTOR", 1);
    this.envId = jsonObject.getString("FOLIO_KAFKA_ENV", "folio");
  }

  public Map<String, String> getProducerProps() {
    Map<String, String> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaUrl());
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    return producerProps;
  }

  public Map<String, String> getConsumerProps() {
    Map<String, String> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaUrl());
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, SimpleConfigurationReader.getValue(KAFKA_CONSUMER_AUTO_OFFSET_RESET, KAFKA_CONSUMER_AUTO_OFFSET_RESET_DEFAULT));
    consumerProps.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, SimpleConfigurationReader.getValue(KAFKA_CONSUMER_METADATA_MAX_AGE, KAFKA_CONSUMER_METADATA_MAX_AGE_DEFAULT));
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    return consumerProps;
  }

  public String getKafkaUrl() {
    return kafkaHost + ":" + kafkaPort;
  }

  public int getNumberOfPartitions() {
    return Integer.parseInt(SimpleConfigurationReader.getValue(KAFKA_NUMBER_OF_PARTITIONS, KAFKA_NUMBER_OF_PARTITIONS_DEFAULT));
  }


}
