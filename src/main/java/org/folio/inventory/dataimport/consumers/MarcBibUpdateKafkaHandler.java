package org.folio.inventory.dataimport.consumers;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.folio.inventory.EntityLinksKafkaTopic.LINKS_STATS;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_REQUEST_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_USER_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_TYPE;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersToMap;
import static org.folio.rest.jaxrs.model.LinkUpdateReport.Status.FAIL;
import static org.folio.rest.jaxrs.model.LinkUpdateReport.Status.SUCCESS;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.MappingMetadataDto;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.SimpleKafkaProducerManager;
import org.folio.kafka.services.KafkaProducerRecordBuilder;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.LinkUpdateReport;
import org.folio.rest.jaxrs.model.MarcBibUpdate;
import org.folio.rest.jaxrs.model.Record;

public class MarcBibUpdateKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(MarcBibUpdateKafkaHandler.class);
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingParameters and mapping rules snapshots were not found by jobId '%s'";
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();
  private static final AtomicLong INDEXER = new AtomicLong();
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "3"));

  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final MappingMetadataCache mappingMetadataCache;
  private final KafkaConfig kafkaConfig;
  private final Vertx vertx;
  private final int maxDistributionNumber;

  public MarcBibUpdateKafkaHandler(Vertx vertx, int maxDistributionNumber, KafkaConfig kafkaConfig,
                                   InstanceUpdateDelegate instanceUpdateDelegate,
                                   MappingMetadataCache mappingMetadataCache) {
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
    this.instanceUpdateDelegate = instanceUpdateDelegate;
    this.maxDistributionNumber = maxDistributionNumber;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> consumerRecord) {
    try {
      var instanceEvent = OBJECT_MAPPER.readValue(consumerRecord.value(), MarcBibUpdate.class);
      var headers = kafkaHeadersToMap(consumerRecord.headers());
      Map<String, String> metaDataPayload = new HashMap<>();

      LOGGER.info("Event payload has been received with event type: {} by jobId: {}", instanceEvent.getType(), instanceEvent.getJobId());

      if (isNull(instanceEvent.getRecord()) || !MarcBibUpdate.Type.UPDATE.equals(instanceEvent.getType())) {
        String message = format("Event message does not contain required data to update Instance by jobId: '%s'", instanceEvent.getJobId());
        LOGGER.error(message);
        return Future.failedFuture(message);
      }

      Promise<String> finalPromise = Promise.promise();
      processEvent(instanceEvent, headers, metaDataPayload)
        .onComplete(ar -> processUpdateResult(ar, consumerRecord, instanceEvent, metaDataPayload, finalPromise));
      return finalPromise.future();
    } catch (Exception ex) {
      LOGGER.error(format("Failed to process kafka record from topic %s", consumerRecord.topic()), ex);
      return Future.failedFuture(ex);
    }
  }

  private Future<Instance> processEvent(MarcBibUpdate instanceEvent, Map<String, String> headers, Map<String, String> metaDataPayload) {
    Context context = constructContext(instanceEvent.getTenant(), headers.get(OKAPI_TOKEN_HEADER), headers.get(OKAPI_URL_HEADER),
      headers.get(OKAPI_USER_ID), headers.get(OKAPI_REQUEST_ID));
    Record marcBibRecord = instanceEvent.getRecord();
    var jobId = instanceEvent.getJobId();

    io.vertx.core.Context vertxContext = Vertx.currentContext();
    if(vertxContext == null) {
      return Future.failedFuture("handle:: operation must be executed by a Vertx thread");
    }

    return vertxContext.owner().executeBlocking(
        () -> {
          var mappingMetadataDto =
            mappingMetadataCache.getByRecordTypeBlocking(jobId, context, MARC_BIB_RECORD_TYPE)
              .orElseThrow(() -> new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobId)));
          ensureEventPayloadWithMappingMetadata(metaDataPayload, mappingMetadataDto);
          return instanceUpdateDelegate.handleBlocking(metaDataPayload, marcBibRecord, context);
        }
      )
      .onSuccess(result -> LOGGER.debug("handle:: Instance update was successful"))
      .onFailure(error -> LOGGER.warn("handle:: Error during instance update", error));
  }

  private void processUpdateResult(AsyncResult<Instance> result,
                                   KafkaConsumerRecord<String, String> consumerRecord,
                                   MarcBibUpdate instanceEvent,
                                   Map<String, String> eventPayload,
                                   Promise<String> promise) {
    if (result.failed() && result.cause() instanceof OptimisticLockingException) {
      var headers = kafkaHeadersToMap(consumerRecord.headers());
      processOLError(consumerRecord, instanceEvent, promise, eventPayload, headers);
      return;
    }

    LinkUpdateReport linkUpdateReport;
    if (result.succeeded()) {
      linkUpdateReport = mapToLinkReport(instanceEvent, null);
      promise.complete(consumerRecord.key());
    } else {
      var errorCause = result.cause();
      LOGGER.error("Failed to update instance by jobId {}:{}", instanceEvent.getJobId(), errorCause);
      linkUpdateReport = mapToLinkReport(instanceEvent, errorCause.getMessage());
      promise.fail(errorCause);
    }

    sendEventToKafka(linkUpdateReport, consumerRecord.headers());
  }

  private void processOLError(KafkaConsumerRecord<String, String> consumerRecord,
                              MarcBibUpdate instanceEvent,
                              Promise<String> promise,
                              Map<String, String> eventPayload,
                              Map<String, String> headers) {
    int retryNumber = Optional.ofNullable(eventPayload.get(CURRENT_RETRY_NUMBER))
      .map(Integer::parseInt)
      .orElse(0);
    if (retryNumber < MAX_RETRIES_COUNT) {
      eventPayload.put(CURRENT_RETRY_NUMBER, String.valueOf(retryNumber + 1));
      LOGGER.warn("Optimistic Locking Error on updating Instance, jobId: {},  Retry Instance update", instanceEvent.getJobId());

      processEvent(instanceEvent, headers, eventPayload)
        .onComplete(ar -> processUpdateResult(ar, consumerRecord, instanceEvent, eventPayload, promise));
      return;
    }

    eventPayload.remove(CURRENT_RETRY_NUMBER);
    String errMessage = format("Optimistic Locking Error, current retry number: %s exceeded the given max retry attempt of %s for Instance update", retryNumber, MAX_RETRIES_COUNT);
    LOGGER.error(errMessage);
    promise.fail(errMessage);

    var linkUpdateReport = mapToLinkReport(instanceEvent, errMessage);
    sendEventToKafka(linkUpdateReport, consumerRecord.headers());
  }

  private void sendEventToKafka(LinkUpdateReport linkUpdateReport, List<KafkaHeader> kafkaHeaders) {
    try {
      var kafkaRecord = createKafkaProducerRecord(linkUpdateReport, kafkaHeaders);
      var producer = createProducer(LINKS_STATS.fullTopicName(linkUpdateReport.getTenant()), kafkaConfig);
      producer.send(kafkaRecord)
        .eventually(producer::flush)
        .eventually(producer::close)
        .onSuccess(res -> LOGGER.info("Event with type {}, jobId {} was sent to kafka", LINKS_STATS.topicName(), linkUpdateReport.getJobId()))
        .onFailure(err -> {
          var cause = err.getCause();
          LOGGER.info("Failed to sent event {} for jobId {}, cause: {}", LINKS_STATS.topicName(), linkUpdateReport.getJobId(), cause);
        });
    } catch (Exception e) {
      LOGGER.error("Failed to send an event for eventType {}, jobId {}, cause {}", LINKS_STATS.topicName(), linkUpdateReport.getJobId(), e);
    }
  }

  private KafkaProducerRecord<String, String> createKafkaProducerRecord(LinkUpdateReport linkUpdateReport, List<KafkaHeader> kafkaHeaders) {
    var key = String.valueOf(INDEXER.incrementAndGet() % maxDistributionNumber);
    var kafkaRecord = new KafkaProducerRecordBuilder<String, Object>(linkUpdateReport.getTenant())
      .key(key)
      .value(linkUpdateReport)
      .topic(LINKS_STATS.fullTopicName(linkUpdateReport.getTenant()))
      .build();

    kafkaRecord.addHeaders(kafkaHeaders);
    return kafkaRecord;
  }

  private void ensureEventPayloadWithMappingMetadata(Map<String, String> eventPayload, MappingMetadataDto mappingMetadataDto) {
    eventPayload.put(MAPPING_RULES_KEY, mappingMetadataDto.getMappingRules());
    eventPayload.put(MAPPING_PARAMS_KEY, mappingMetadataDto.getMappingParams());
  }

  private KafkaProducer<String, String> createProducer(String eventType, KafkaConfig kafkaConfig) {
    return new SimpleKafkaProducerManager(vertx, kafkaConfig).createShared(eventType);
  }

  private LinkUpdateReport mapToLinkReport(MarcBibUpdate marcBibUpdate, String errMessage) {
    var instanceId = AdditionalFieldsUtil.getValue(marcBibUpdate.getRecord(), TAG_999, SUBFIELD_I)
      .orElse(null);
    return new LinkUpdateReport()
      .withJobId(marcBibUpdate.getJobId())
      .withInstanceId(instanceId)
      .withLinkIds(marcBibUpdate.getLinkIds())
      .withStatus(errMessage == null ? SUCCESS : FAIL)
      .withFailCause(errMessage)
      .withTenant(marcBibUpdate.getTenant())
      .withTs(marcBibUpdate.getTs());
  }
}
