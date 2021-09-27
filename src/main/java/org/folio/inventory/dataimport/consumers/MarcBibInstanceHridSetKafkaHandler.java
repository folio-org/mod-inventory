package org.folio.inventory.dataimport.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

public class MarcBibInstanceHridSetKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(MarcBibInstanceHridSetKafkaHandler.class);
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingParameters and mapping rules snapshots were not found by jobExecutionId '%s'";
  private static final String MARC_KEY = "MARC_BIB";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  public static final String JOB_EXECUTION_ID_KEY = "JOB_EXECUTION_ID";
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();
  private static final String CORRELATION_ID_HEADER = "correlationId";

  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final KafkaInternalCache kafkaInternalCache;
  private final MappingMetadataCache mappingMetadataCache;

  public MarcBibInstanceHridSetKafkaHandler(InstanceUpdateDelegate instanceUpdateDelegate, KafkaInternalCache kafkaInternalCache, MappingMetadataCache mappingMetadataCache) {
    this.instanceUpdateDelegate = instanceUpdateDelegate;
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
        HashMap<String, String> eventPayload = OBJECT_MAPPER.readValue(event.getEventPayload(), HashMap.class);
        Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
        String correlationId = headersMap.get(CORRELATION_ID_HEADER);
        LOGGER.info(format("Event payload has been received with event type: %s and correlationId: %s", event.getEventType(), correlationId));

        if (isEmpty(eventPayload.get(MARC_KEY))) {
          String message = "Event payload does not contain required data to update Instance";
          LOGGER.error(message);
          return Future.failedFuture(message);
        }

        Context context = EventHandlingUtil.constructContext(headersMap.get(OKAPI_TENANT_HEADER), headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER));
        Record marcRecord = new JsonObject(eventPayload.get(MARC_KEY)).mapTo(Record.class);
        String jobExecutionId = eventPayload.get(JOB_EXECUTION_ID_KEY);

        mappingMetadataCache.get(jobExecutionId, context)
          .map(metadataOptional -> metadataOptional.orElseThrow(() ->
            new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobExecutionId))))
          .onSuccess(mappingMetadataDto -> ensureEventPayloadWithMappingMetadata(eventPayload, mappingMetadataDto))
          .compose(v -> instanceUpdateDelegate.handle(eventPayload, marcRecord, context))
          .onComplete(ar -> {
            if (ar.succeeded()) {
              promise.complete(record.key());
            } else {
              LOGGER.error("Failed to process data import event payload", ar.cause());
              promise.fail(ar.cause());
            }
          });
        return promise.future();
      }
    } catch (Exception e) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", record.topic()), e);
      return Future.failedFuture(e);
    }
    return Future.succeededFuture();
  }

  private void ensureEventPayloadWithMappingMetadata(HashMap<String, String> eventPayload, MappingMetadataDto mappingMetadataDto) {
    eventPayload.put(MAPPING_RULES_KEY, mappingMetadataDto.getMappingRules());
    eventPayload.put(MAPPING_PARAMS_KEY, mappingMetadataDto.getMappingParams());
  }

}
