package org.folio.inventory.dataimport.consumers;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.MappingMetadataDto;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.AuthorityUpdateDelegate;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;

public class MarcAuthorityRecordHridSetKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(MarcAuthorityRecordHridSetKafkaHandler.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingParameters and mapping rules snapshots were not found by jobExecutionId '%s'";
  private static final String MARC_KEY = "MARC_AUTHORITY";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String JOB_EXECUTION_ID_KEY = "JOB_EXECUTION_ID";
  private static final String RECORD_ID_HEADER = "recordId";

  private final AuthorityUpdateDelegate authorityUpdateDelegate;
  private final KafkaInternalCache kafkaInternalCache;
  private final MappingMetadataCache mappingMetadataCache;

  public MarcAuthorityRecordHridSetKafkaHandler(AuthorityUpdateDelegate authorityUpdateDelegate,
                                                KafkaInternalCache kafkaInternalCache,
                                                MappingMetadataCache mappingMetadataCache) {
    this.authorityUpdateDelegate = authorityUpdateDelegate;
    this.kafkaInternalCache = kafkaInternalCache;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> kafkaRecord) {
    try {
      Event event = OBJECT_MAPPER.readValue(kafkaRecord.value(), Event.class);
      if (!kafkaInternalCache.containsByKey(event.getId())) {
        @SuppressWarnings("unchecked")
        HashMap<String, String> eventPayload = OBJECT_MAPPER.readValue(event.getEventPayload(), HashMap.class);
        Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(kafkaRecord.headers());
        String recordId = headersMap.get(RECORD_ID_HEADER);
        kafkaInternalCache.putToCache(event.getId());

        if (isEmpty(eventPayload.get(MARC_KEY))) {
          String message = "Event payload does not contain required data to update Instance";
          LOGGER.error(message);
          return Future.failedFuture(message);
        }

        LOGGER.info("Event payload has been received with event type: {} and recordId: {}", event.getEventType(), recordId);

        Context context = EventHandlingUtil.constructContext(headersMap.get(OKAPI_TENANT_HEADER), headersMap.get(OKAPI_TOKEN_HEADER),
          headersMap.get(OKAPI_URL_HEADER));
        Record marcRecord = new JsonObject(eventPayload.get(MARC_KEY)).mapTo(Record.class);
        String jobExecutionId = eventPayload.get(JOB_EXECUTION_ID_KEY);

        return processDataImport(kafkaRecord, eventPayload, context, marcRecord, jobExecutionId);
      }
    } catch (Exception e) {
      LOGGER.error(format("Failed to process data import kafka kafkaRecord from topic %s", kafkaRecord.topic()), e);
      return Future.failedFuture(e);
    }
    return Future.succeededFuture();
  }

  private Future<String> processDataImport(KafkaConsumerRecord<String, String> kafkaRecord,
                                           HashMap<String, String> eventPayload,
                                           Context context,
                                           Record marcRecord,
                                           String jobExecutionId) {
    Promise<String> promise = Promise.promise();

    mappingMetadataCache.get(jobExecutionId, context)
      .map(metadataOptional -> metadataOptional.orElseThrow(() ->
        new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobExecutionId))))
      .onSuccess(mappingMetadataDto -> ensureEventPayloadWithMappingMetadata(eventPayload, mappingMetadataDto))
      .compose(empty -> authorityUpdateDelegate.handle(eventPayload, marcRecord, context))
      .onComplete(authorityFuture -> {
        if (authorityFuture.succeeded()) {
          promise.complete(kafkaRecord.key());
        } else {
          LOGGER.error("Failed to process data import event payload", authorityFuture.cause());
          promise.fail(authorityFuture.cause());
        }
      });
    return promise.future();
  }

  private void ensureEventPayloadWithMappingMetadata(HashMap<String, String> eventPayload, MappingMetadataDto mappingMetadataDto) {
    eventPayload.put(MAPPING_RULES_KEY, mappingMetadataDto.getMappingRules());
    eventPayload.put(MAPPING_PARAMS_KEY, mappingMetadataDto.getMappingParams());
  }

}
