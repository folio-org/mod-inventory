package org.folio.inventory.dataimport.consumers;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.folio.inventory.EntityLinksKafkaTopic.LINKS_STATS;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_TYPE;
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
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
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
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));

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
      Promise<String> promise = Promise.promise();
      MarcBibUpdate instanceEvent = OBJECT_MAPPER.readValue(consumerRecord.value(), MarcBibUpdate.class);
      Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(consumerRecord.headers());
      HashMap<String, String> metaDataPayload = new HashMap<>();
      var jobId = instanceEvent.getJobId();

      LOGGER.info("Event payload has been received with event type: {} by jobId: {}", instanceEvent.getType(), instanceEvent.getJobId());

      if (isNull(instanceEvent.getRecord()) || !MarcBibUpdate.Type.UPDATE.equals(instanceEvent.getType())) {
        String message = String.format("Event message does not contain required data to update Instance by jobId: '%s'", instanceEvent.getJobId());
        LOGGER.error(message);
        return Future.failedFuture(message);
      }
      Context context = EventHandlingUtil.constructContext(instanceEvent.getTenant(), headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER));
      Record marcBibRecord = instanceEvent.getRecord();

      var mappingMetadataDto =
        mappingMetadataCache.getByRecordTypeBlocking(jobId, context, MARC_BIB_RECORD_TYPE)
          .orElseThrow(() -> new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobId)));
      ensureEventPayloadWithMappingMetadata(metaDataPayload, mappingMetadataDto);
      instanceUpdateDelegate.handleBlocking(metaDataPayload, marcBibRecord, context)
        .onComplete(ar -> processUpdateResult(ar, metaDataPayload, promise, consumerRecord, instanceEvent));

      return promise.future();
    } catch (Exception e) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", consumerRecord.topic()), e);
      return Future.failedFuture(e);
    }
  }

  private void processUpdateResult(AsyncResult<Instance> result,
                                   HashMap<String, String> eventPayload,
                                   Promise<String> promise,
                                   KafkaConsumerRecord<String, String> consumerRecord,
                                   MarcBibUpdate instanceEvent) {
    if (result.failed() && result.cause() instanceof OptimisticLockingException) {
      processOLError(consumerRecord, promise, eventPayload, instanceEvent.getJobId(), result);
      return;
    }

    LinkUpdateReport linkUpdateReport;
    if (result.succeeded()) {
      linkUpdateReport = mapToLinkReport(instanceEvent, null);
      promise.complete(consumerRecord.key());
    } else {
      var errorCause = result.cause();
      LOGGER.error("Failed to update instance by jobId {}:{}", instanceEvent.getJobId(), errorCause);
      eventPayload.remove(CURRENT_RETRY_NUMBER);
      linkUpdateReport = mapToLinkReport(instanceEvent, errorCause.getMessage());
      promise.fail(errorCause);
    }

    sendEventToKafka(linkUpdateReport, consumerRecord.headers());
  }

  private void processOLError(KafkaConsumerRecord<String, String> value,
                              Promise<String> promise,
                              HashMap<String, String> eventPayload,
                              String jobId,
                              AsyncResult<Instance> ar) {
    int currentRetryNumber = Optional.ofNullable(eventPayload.get(CURRENT_RETRY_NUMBER))
      .map(Integer::parseInt).orElse(0);
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      eventPayload.put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("Error updating Instance: {}, jobId: {},  Retry MarcBibUpdateKafkaHandler handler...",
        ar.cause().getMessage(), jobId);
      handle(value).onComplete(result -> {
        if (result.succeeded()) {
          promise.complete(value.key());
        } else {
          promise.fail(result.cause());
        }
      });
    } else {
      eventPayload.remove(CURRENT_RETRY_NUMBER);
      String errMessage = format("Current retry number %s exceeded given number %s for the Instance update", MAX_RETRIES_COUNT, currentRetryNumber);
      LOGGER.error(errMessage);
      promise.fail(new OptimisticLockingException(errMessage));
    }
  }

  private void sendEventToKafka(LinkUpdateReport linkUpdateReport, List<KafkaHeader> kafkaHeaders) {
    try {
      var kafkaRecord = createKafkaProducerRecord(linkUpdateReport, kafkaHeaders);
      KafkaProducer<String, String> producer = createProducer(LINKS_STATS.topicName(), kafkaConfig);
      producer.send(kafkaRecord)
        .<Void>mapEmpty()
        .eventually(v -> producer.flush())
        .eventually(v -> producer.close())
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
    var topicName = formatTopicName(kafkaConfig.getEnvId(), linkUpdateReport.getTenant(), LINKS_STATS.topicName());
    var key = String.valueOf(INDEXER.incrementAndGet() % maxDistributionNumber);
    var kafkaRecord = new KafkaProducerRecordBuilder<String, Object>(linkUpdateReport.getTenant())
      .key(key)
      .value(linkUpdateReport)
      .topic(topicName)
      .build();

    kafkaRecord.addHeaders(kafkaHeaders);
    return kafkaRecord;
  }

  private static String formatTopicName(String env, String tenant, String eventType) {
    return String.join(".", env, tenant, eventType);
  }

  private void ensureEventPayloadWithMappingMetadata(HashMap<String, String> eventPayload, MappingMetadataDto mappingMetadataDto) {
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
