package org.folio.inventory;

import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.cache.CancelledJobsIdsCache;
import org.folio.inventory.support.KafkaConsumerVerticle;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.rest.jaxrs.model.Event;

import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.folio.DataImportEventTypes.DI_JOB_CANCELLED;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

public class CancelledJobExecutionConsumerVerticle extends KafkaConsumerVerticle {

  private static final Logger LOGGER = LogManager.getLogger(CancelledJobExecutionConsumerVerticle.class);
  public static final String TEST_MODE_PARAM = "test.mode.enabled";
  public static final String GROUP_NAME_PREFIX = "DI_JOB_CANCELLED_INVENTORY_GROUP-%s";

  private final CancelledJobsIdsCache cancelledJobsIdsCache;
  private KafkaConsumer<String, String> consumer;
  private String groupName;

  public CancelledJobExecutionConsumerVerticle(CancelledJobsIdsCache cancelledJobsIdsCache) {
    this.cancelledJobsIdsCache = cancelledJobsIdsCache;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    KafkaConfig kafkaConfig = getKafkaConfig();
    Map<String, String> consumerProps = kafkaConfig.getConsumerProps();
    groupName = getGroupName();
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
    SubscriptionDefinition subscriptionDefinition =
      KafkaTopicNameHelper.createSubscriptionDefinition(kafkaConfig.getEnvId(), getDefaultNameSpace(), DI_JOB_CANCELLED.value());
    Pattern pattern = Pattern.compile(subscriptionDefinition.getSubscriptionPattern());

    if (isTestMode()) {
    // set metadata max age to speed up messages retrieval by consumer in tests
      consumerProps.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
    }

    consumer = KafkaConsumer.create(vertx, consumerProps);
    consumer.batchHandler(this::handle)
      .subscribe(pattern)
      .onSuccess(v -> LOGGER.info("start:: Consumer created, consumer group: {}, subscriptionDefinition: {}",
        groupName, subscriptionDefinition))
      .onFailure(e -> LOGGER.error("start:: Consumer creation failed, consumer group: {}, subscriptionDefinition: {}",
        groupName, subscriptionDefinition, e))
      .onComplete(startPromise);
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  private String getGroupName() {
    return GROUP_NAME_PREFIX.formatted(UUID.randomUUID().toString());
  }

  private boolean isTestMode() {
    return Boolean.parseBoolean(context.config().getString(TEST_MODE_PARAM));
  }

  private void handle(KafkaConsumerRecords<String, String> records) {

    for (ConsumerRecord<String, String> kafkaRecord : records.records()) {
      String jobId = Json.decodeValue(kafkaRecord.value(), Event.class).getEventPayload();
      cancelledJobsIdsCache.put(UUID.fromString(jobId));
    }

    KafkaConsumerRecord<String, String> lastRecord = records.recordAt(records.size() - 1);
    LOGGER.info("handle:: Processed cancelled jobs from topic: {}, records batch size: {}",
      lastRecord.topic(), records.size());

    consumer.commit()
      .onSuccess(v -> LOGGER.info("handle:: Committed offset for topic: {}, last record with key: {}, and offset: {}",
        lastRecord.topic(), lastRecord.key(), lastRecord.offset()))
      .onFailure(e -> LOGGER.error("handle:: Failed to commit offset for topic: {}, last record with key: {}, and offset: {}",
        lastRecord.topic(), lastRecord.key(), lastRecord.offset()));
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumer.close()
      .onSuccess(v -> LOGGER.info("stop:: Consumer was closed, consumer group: '{}'", groupName))
      .onFailure(e -> LOGGER.error("stop:: Consumer was not closed, consumer group: '{}'", groupName, e))
      .onComplete(stopPromise);
  }

}
