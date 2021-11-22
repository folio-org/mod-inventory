package org.folio.inventory.dataimport.consumers;

import static java.lang.String.format;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.HoldingsUpdateDelegate;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Event;
import org.folio.MappingMetadataDto;
import org.folio.rest.jaxrs.model.Record;

/**
 * This handler is a part of post-processing logic that is intended for handling the creation of Holdings record.
 * The handler is intended to process the event that is thrown on setting HRID to the Holdings record.
 * Execution steps:
 * 1. Create a new Holdings record by the way of mapping the incoming SRS Marc record using Mapping profile;
 * 2. Retrieve an existing Inventory Holdings record;
 * 3. Merge a new record (from step 1) with an existing record (from step 2);
 * 4. Update an existing record (from step 2) with a result of merge (from step 3);
 */
public class MarcHoldingsRecordHridSetKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(MarcHoldingsRecordHridSetKafkaHandler.class);
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingParameters and mapping rules snapshots were not found by jobExecutionId '%s'";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String MARC_KEY = "MARC_HOLDINGS";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  public static final String JOB_EXECUTION_ID_KEY = "JOB_EXECUTION_ID";
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();

  private final HoldingsUpdateDelegate holdingsRecordUpdateDelegate;
  private final KafkaInternalCache kafkaInternalCache;
  private final MappingMetadataCache mappingMetadataCache;

  public MarcHoldingsRecordHridSetKafkaHandler(HoldingsUpdateDelegate holdingsRecordUpdateDelegate,
                                               KafkaInternalCache kafkaInternalCache,
                                               MappingMetadataCache mappingMetadataCache) {
    this.holdingsRecordUpdateDelegate = holdingsRecordUpdateDelegate;
    this.kafkaInternalCache = kafkaInternalCache;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      Event event = OBJECT_MAPPER.readValue(record.value(), Event.class);
      if (!kafkaInternalCache.containsByKey(event.getId())) {
        kafkaInternalCache.putToCache(event.getId());
        @SuppressWarnings("unchecked")
        HashMap<String, String> eventPayload =
          OBJECT_MAPPER.readValue(event.getEventPayload(), HashMap.class);
        Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
        String recordId = headersMap.get(RECORD_ID_HEADER);
        LOGGER.info("Event payload has been received with event type: {} and recordId: {}", event.getEventType(),
          recordId);

        if (isEmpty(eventPayload.get(MARC_KEY))) {
          String message = "Event payload does not contain required data to update Holdings";
          LOGGER.error(message);
          return Future.failedFuture(message);
        }

        Context context = constructContext(headersMap.get(OKAPI_TENANT_HEADER), headersMap.get(OKAPI_TOKEN_HEADER),
          headersMap.get(OKAPI_URL_HEADER));
        String jobExecutionId = eventPayload.get(JOB_EXECUTION_ID_KEY);
        Record marcRecord = Json.decodeValue(eventPayload.get(MARC_KEY), Record.class);

        mappingMetadataCache.get(jobExecutionId, context)
          .map(metadataOptional -> metadataOptional.orElseThrow(() ->
            new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobExecutionId))))
          .onSuccess(mappingMetadataDto -> ensureEventPayloadWithMappingMetadata(eventPayload, mappingMetadataDto))
          .compose(v -> holdingsRecordUpdateDelegate.handle(eventPayload, marcRecord, context))
          .onComplete(ar -> {
            if (ar.succeeded()) {
              promise.complete(record.key());
            } else {
              LOGGER.error("Failed to process data import event payload ", ar.cause());
              promise.fail(ar.cause());
            }
          });
        return promise.future();
      }
    } catch (Exception e) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s ", record.topic()), e);
      return Future.failedFuture(e);
    }
    return Future.succeededFuture();
  }

  private void ensureEventPayloadWithMappingMetadata(HashMap<String, String> eventPayload, MappingMetadataDto mappingMetadataDto) {
    eventPayload.put(MAPPING_RULES_KEY, mappingMetadataDto.getMappingRules());
    eventPayload.put(MAPPING_PARAMS_KEY, mappingMetadataDto.getMappingParams());
  }
}
