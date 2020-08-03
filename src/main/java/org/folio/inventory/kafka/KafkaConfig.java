package org.folio.inventory.kafka;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

public class KafkaConfig {

  private String kafkaHost;
  private String kafkaPort;
  private String okapiUrl;
  private int replicationFactor;
  private int numberOfPartitions;
  private String envId;

  public KafkaConfig(JsonObject jsonObject) {
    this.kafkaHost = jsonObject.getString("KAFKA_HOST", "10.0.2.15");
    this.kafkaPort = jsonObject.getString("KAFKA_PORT", "9092");
    this.okapiUrl = jsonObject.getString("OKAPI_URL", "http://10.0.2.15:9130");
    this.replicationFactor = jsonObject.getInteger("REPLICATION_FACTOR", 1);
    this.numberOfPartitions = jsonObject.getInteger("NUMBER_OF_PARTITIONS", 1);
    this.envId = jsonObject.getString("ENV", "folio");
  }

  public String getKafkaHost() {
    return kafkaHost;
  }

  public String getKafkaPort() {
    return kafkaPort;
  }

  public String getOkapiUrl() {
    return okapiUrl;
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
    //TODO: all commits in Kafka Consumers must be manual!
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    return consumerProps;
  }

  public String getKafkaUrl() {
    return kafkaHost + ":" + kafkaPort;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public int getNumberOfPartitions() {
    return numberOfPartitions;
  }

  public String getEnvId() {
    return envId;
  }

  public KafkaAdminClient kafkaAdminClient(Vertx vertx, KafkaConfig config) {
    Map<String, String> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaUrl());
    return KafkaAdminClient.create(vertx, configs);
  }
}
