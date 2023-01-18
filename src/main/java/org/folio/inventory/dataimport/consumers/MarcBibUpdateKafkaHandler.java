package org.folio.inventory.dataimport.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.LinkUpdateReport;
import org.folio.MappingMetadataDto;
import org.folio.MarcBibUpdate;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.LinkUpdateReport.Status.FAIL;
import static org.folio.LinkUpdateReport.Status.SUCCESS;
import static org.folio.inventory.EntityLinksKafkaTopic.LINKS_STATS;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

public class MarcBibUpdateKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(MarcBibUpdateKafkaHandler.class);
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingParameters and mapping rules snapshots were not found by jobId '%s'";
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();
  private static final AtomicLong INDEXER = new AtomicLong();
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String MARC_BIB_RECORD_TYPE = "marc-bib";

  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final MappingMetadataCache mappingMetadataCache;
  private final KafkaProducer<String, String> producer;
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

    producer = createProducer(LINKS_STATS.topicName(), kafkaConfig);
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      MarcBibUpdate instanceEvent = OBJECT_MAPPER.readValue(record.value(), MarcBibUpdate.class);
      Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
      HashMap<String, String> metaDataPayload = new HashMap<>();

      LOGGER.info("Event payload has been received with event type: {} by jobId: {}", instanceEvent.getType(), instanceEvent.getJobId());

      if (isEmpty(instanceEvent.getRecord()) || !MarcBibUpdate.Type.UPDATE.equals(instanceEvent.getType())) {
        String message = String.format("Event message does not contain required data to update Instance by jobId: '%s'", instanceEvent.getJobId());
        LOGGER.error(message);
        return Future.failedFuture(message);
      }
      Context context = EventHandlingUtil.constructContext(instanceEvent.getTenant(), headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER));
      Record marcBibRecord = new JsonObject(instanceEvent.getRecord()).mapTo(Record.class);

      mappingMetadataCache.getByRecordType(instanceEvent.getJobId(), context, MARC_BIB_RECORD_TYPE)
        .map(metadataOptional -> metadataOptional.orElseThrow(() ->
          new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, instanceEvent.getJobId()))))
        .onSuccess(mappingMetadataDto -> ensureEventPayloadWithMappingMetadata(metaDataPayload, mappingMetadataDto))
        .compose(v -> instanceUpdateDelegate.handle(metaDataPayload, marcBibRecord, context))
        .onComplete(ar -> processUpdateResult(ar, promise, record, instanceEvent, marcBibRecord));
      return promise.future();
    } catch (Exception e) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", record.topic()), e);
      return Future.failedFuture(e);
    }
  }

  private void processUpdateResult(AsyncResult<Instance> result, Promise<String> promise,
                                   KafkaConsumerRecord<String, String> consumerRecord,
                                   MarcBibUpdate instanceEvent, Record marcBibRecord) {
    LinkUpdateReport linkUpdateReport;
    if (result.succeeded()) {
      linkUpdateReport = mapToLinkReport(instanceEvent, marcBibRecord.getId(), null);
      promise.complete(consumerRecord.key());
    } else {
      var errorCause = result.cause();
      linkUpdateReport = mapToLinkReport(instanceEvent, marcBibRecord.getId(), errorCause.getMessage());
      LOGGER.error("Failed to update instance by jobId {}:{}", instanceEvent.getJobId(), errorCause);
      promise.fail(errorCause);
    }
    sendEventToKafka(linkUpdateReport, consumerRecord.headers());
  }

  private void sendEventToKafka(LinkUpdateReport linkUpdateReport, List<KafkaHeader> kafkaHeaders) {
    try {
      var kafkaRecord = createKafkaProducerRecord(linkUpdateReport, kafkaHeaders);
      producer.write(kafkaRecord, ar -> {
        if (ar.succeeded()) {
          LOGGER.debug("Event with type {}, jobId {} was sent to kafka", LINKS_STATS.topicName(), linkUpdateReport.getJobId());
        } else {
          var cause = ar.cause();
          LOGGER.error("Failed to sent event {} for jobId {}, cause: {}", LINKS_STATS.topicName(), linkUpdateReport.getJobId(), cause);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to send an event for eventType {}, jobId {}, cause {}", LINKS_STATS.topicName(), linkUpdateReport.getJobId(), e);
    }
  }

  private KafkaProducerRecord<String, String> createKafkaProducerRecord(LinkUpdateReport linkUpdateReport, List<KafkaHeader> kafkaHeaders) {
    var topicName = formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), linkUpdateReport.getTenant(), LINKS_STATS.topicName());
    var key = String.valueOf(INDEXER.incrementAndGet() % maxDistributionNumber);
    var kafkaRecord = KafkaProducerRecord.create(topicName, key, Json.encode(linkUpdateReport));
    kafkaRecord.addHeaders(kafkaHeaders);

    return kafkaRecord;
  }

  private void ensureEventPayloadWithMappingMetadata(HashMap<String, String> eventPayload, MappingMetadataDto mappingMetadataDto) {
    eventPayload.put(MAPPING_RULES_KEY, mappingMetadataDto.getMappingRules());
    eventPayload.put(MAPPING_PARAMS_KEY, mappingMetadataDto.getMappingParams());
  }

  private KafkaProducer<String, String> createProducer(String eventType, KafkaConfig kafkaConfig) {
    String producerName = eventType + "_Producer";
    return KafkaProducer.createShared(vertx, producerName, kafkaConfig.getProducerProps());
  }

  private LinkUpdateReport mapToLinkReport(MarcBibUpdate marcBibUpdate, String instanceId, String errMessage) {
    var report = new LinkUpdateReport();
    report.setJobId(marcBibUpdate.getJobId());
    report.setInstanceId(instanceId);
    report.setLinkIds(marcBibUpdate.getLinkIds());
    report.setStatus(errMessage == null ? SUCCESS : FAIL);
    report.setFailCause(errMessage);
    report.setTenant(marcBibUpdate.getTenant());
    report.setTs(marcBibUpdate.getTs());
    return report;
  }
}