package org.folio.inventory.dataimport.consumers;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_REQUEST_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_USER_ID;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_FORMAT;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.MappingMetadataDto;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

public class MarcBibInstanceHridSetKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(MarcBibInstanceHridSetKafkaHandler.class);
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingParameters and mapping rules snapshots were not found by jobExecutionId '%s'";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String JOB_EXECUTION_ID_HEADER = "JOB_EXECUTION_ID";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));
  private static final String CENTRAL_TENANT_ID_KEY = "CENTRAL_TENANT_ID";

  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final MappingMetadataCache mappingMetadataCache;

  public MarcBibInstanceHridSetKafkaHandler(InstanceUpdateDelegate instanceUpdateDelegate, MappingMetadataCache mappingMetadataCache) {
    this.instanceUpdateDelegate = instanceUpdateDelegate;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      Event event = OBJECT_MAPPER.readValue(record.value(), Event.class);
      @SuppressWarnings("unchecked")
      HashMap<String, String> eventPayload = OBJECT_MAPPER.readValue(event.getEventPayload(), HashMap.class);
      Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
      String recordId = headersMap.get(RECORD_ID_HEADER);
      String chunkId = headersMap.get(CHUNK_ID_HEADER);
      String jobExecutionId = eventPayload.get(JOB_EXECUTION_ID_HEADER);
      LOGGER.info("Event payload has been received with event type: {}, recordId: {} by jobExecution: {} and chunkId: {}", event.getEventType(), recordId, jobExecutionId, chunkId);

      if (isEmpty(eventPayload.get(MARC_BIB_RECORD_FORMAT))) {
        String message = format("Event payload does not contain required data to update Instance with event type: '%s', recordId: '%s' by jobExecution: '%s' and chunkId: '%s'", event.getEventType(), recordId, jobExecutionId, chunkId);
        LOGGER.error(message);
        return Future.failedFuture(message);
      }

      Context context = EventHandlingUtil.constructContext(headersMap.get(OKAPI_TENANT_HEADER), headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER),
        headersMap.get(OKAPI_USER_ID), headersMap.get(OKAPI_REQUEST_ID));
      Record marcRecord = new JsonObject(eventPayload.get(MARC_BIB_RECORD_FORMAT)).mapTo(Record.class);

      mappingMetadataCache.get(jobExecutionId, context)
        .map(metadataOptional -> metadataOptional.orElseThrow(() ->
          new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobExecutionId))))
        .onSuccess(mappingMetadataDto -> ensureEventPayloadWithMappingMetadata(eventPayload, mappingMetadataDto))
        .compose(v -> updateInstance(eventPayload, marcRecord, headersMap))
        .onComplete(ar -> {
          if (ar.succeeded()) {
            eventPayload.remove(CURRENT_RETRY_NUMBER);
            promise.complete(record.key());
          } else {
            if (ar.cause() instanceof OptimisticLockingException) {
              processOLError(record, promise, eventPayload, ar);
            } else {
              eventPayload.remove(CURRENT_RETRY_NUMBER);
              LOGGER.error("Failed to set MarcBib Hrid by jobExecutionId: {} recordId: {} chunkId: {}", jobExecutionId, recordId, chunkId, ar.cause());
              promise.fail(ar.cause());
            }
          }
        });
      return promise.future();
    } catch (Exception e) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", record.topic()), e);
      return Future.failedFuture(e);
    }
  }

  private Future<Instance> updateInstance(HashMap<String, String> eventPayload, Record marcRecord, Map<String, String> headersMap) {
    String tenantId = eventPayload.getOrDefault(CENTRAL_TENANT_ID_KEY, eventPayload.get(OKAPI_TENANT_HEADER));
    Context context = EventHandlingUtil.constructContext(tenantId, headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER),
      headersMap.get(OKAPI_USER_ID), headersMap.get(OKAPI_REQUEST_ID));
    return instanceUpdateDelegate.handle(eventPayload, marcRecord, context);
  }

  private void processOLError(KafkaConsumerRecord<String, String> value, Promise<String> promise, HashMap<String, String> eventPayload, AsyncResult<Instance> ar) {
    int currentRetryNumber = eventPayload.get(CURRENT_RETRY_NUMBER) == null
      ? 0 : Integer.parseInt(eventPayload.get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      eventPayload.put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("Error updating Instance - {}. Retry MarcBibInstanceHridSetKafkaHandler handler...", ar.cause().getMessage());
      handle(value).onComplete(res -> {
        if (res.succeeded()) {
          promise.complete(value.key());
        } else {
          promise.fail(res.cause());
        }
      });
    } else {
      eventPayload.remove(CURRENT_RETRY_NUMBER);
      String errMessage = format("Current retry number %s exceeded given number %s for the Instance update", MAX_RETRIES_COUNT, currentRetryNumber);
      LOGGER.error(errMessage);
      promise.fail(new OptimisticLockingException(errMessage));
    }
  }

  private void ensureEventPayloadWithMappingMetadata(HashMap<String, String> eventPayload, MappingMetadataDto mappingMetadataDto) {
    eventPayload.put(MAPPING_RULES_KEY, mappingMetadataDto.getMappingRules());
    eventPayload.put(MAPPING_PARAMS_KEY, mappingMetadataDto.getMappingParams());
  }

}
