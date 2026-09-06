package org.folio.inventory.verticle;

import static org.folio.DataImportEventTypes.DI_JOB_CANCELLED;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.cache.CancelledJobsIdsCache;
import org.folio.inventory.dataimport.util.ConsumerWrapperUtil;
import org.folio.inventory.support.KafkaConsumerVerticle;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.headers.FolioKafkaHeaders;
import org.folio.rest.jaxrs.model.Event;

public class CancelledJobExecutionConsumerVerticle extends KafkaConsumerVerticle {

  private static final Logger LOGGER = LogManager.getLogger(CancelledJobExecutionConsumerVerticle.class);
  private static final String LOAD_LIMIT_PROPERTY = "CancelledJobExecutionConsumer";
  private static final String DEFAULT_LOAD_LIMIT = "1000";

  private final CancelledJobsIdsCache cancelledJobsIdsCache = CancelledJobsIdsCache.getInstance();

  @Override
  public void start(Promise<Void> startPromise) {
    String moduleName = getModuleName();
    String groupName = KafkaTopicNameHelper.formatGroupName(DI_JOB_CANCELLED.value(), moduleName);

    KafkaConsumerWrapper<String, String> consumerWrapper =
      createConsumer(DI_JOB_CANCELLED.value(), LOAD_LIMIT_PROPERTY);
    consumerWrapper.start(this::handle, moduleName)
      .onSuccess(v ->
        LOGGER.info("start:: CancelledJobExecutionConsumerVerticle verticle was started, consumer group: '{}'",
          groupName))
      .onFailure(e -> LOGGER.error("start:: Failed to start CancelledJobExecutionConsumerVerticle verticle", e))
      .onComplete(startPromise);
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  @Override
  protected String getDefaultLoadLimit() {
    return DEFAULT_LOAD_LIMIT;
  }

  /**
   * Constructs a unique module name with pseudo-random suffix.
   * This ensures that each instance of the module will have own consumer group
   * and will consume all messages from the topic.
   *
   * @return unique module name string.
   */
  private String getModuleName() {
    return ConsumerWrapperUtil.constructModuleName() + "-" + UUID.randomUUID();
  }

  private Future<String> handle(KafkaConsumerRecord<String, String> kafkaRecord) {
    var recordKey = kafkaRecord.key();
    var recordTopic = kafkaRecord.topic();
    try {
      String tenantId = extractTenantHeader(kafkaRecord.headers());
      LOGGER.debug("handle:: Received cancelled job event, key: '{}', tenantId: '{}'", recordKey, tenantId);

      String jobId = Json.decodeValue(kafkaRecord.value(), Event.class).getEventPayload();
      cancelledJobsIdsCache.put(jobId);
      LOGGER.info("handle:: Processed cancelled job, jobId: '{}', tenantId: '{}', topic: '{}'",
        jobId, tenantId, recordTopic);
      return Future.succeededFuture(recordKey);
    } catch (Exception e) {
      LOGGER.warn("handle:: Failed to process cancelled job, key: '{}', from topic: '{}'",
        recordKey, recordTopic, e);
      return Future.failedFuture(e);
    }
  }

  private String extractTenantHeader(List<KafkaHeader> headers) {
    return headers.stream()
      .filter(header -> header.key().equals(FolioKafkaHeaders.TENANT_ID))
      .findAny()
      .map(header -> header.value().toString())
      .orElse(null);
  }
}
