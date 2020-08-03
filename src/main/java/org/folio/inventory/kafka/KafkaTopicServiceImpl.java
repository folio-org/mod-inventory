package org.folio.inventory.kafka;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;


public class KafkaTopicServiceImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicServiceImpl.class);

  private KafkaAdminClient kafkaAdminClient;
  private KafkaConfig kafkaConfig;

  public KafkaTopicServiceImpl(KafkaAdminClient kafkaAdminClient, KafkaConfig kafkaConfig) {
    this.kafkaAdminClient = kafkaAdminClient;
    this.kafkaConfig = kafkaConfig;
  }

  public Future<Boolean> createTopics(List<String> eventTypes, String tenantId) {
    Future<Boolean> promise = Future.future();
    List<NewTopic> topics = eventTypes.stream()
      .map(eventType -> new NewTopic(new PubSubConfig(kafkaConfig.getEnvId(), tenantId, eventType).getTopicName(), kafkaConfig.getNumberOfPartitions(), (short) kafkaConfig.getReplicationFactor()))
      .collect(Collectors.toList());
    kafkaAdminClient.createTopics(topics, ar -> {
      if (ar.succeeded()) {
        LOGGER.info("Created topics: [{}]", StringUtils.join(eventTypes, ","));
        promise.complete(true);
      } else {
        LOGGER.info("Some of the topics [{}] were not created. Cause: {}", StringUtils.join(eventTypes, ","), ar.cause().getMessage());
        promise.complete(false);
      }
    });
    return promise;
  }
}
