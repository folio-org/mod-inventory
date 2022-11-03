package org.folio.inventory.dataimport.consumers;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
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
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Record;

public class MarcBibUpdateKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(MarcBibUpdateKafkaHandler.class);
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingParameters and mapping rules snapshots were not found by jobId '%s'";
  private static final String RECORD_KEY = "record";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();
  private static final String JOB_ID = "jobId";
  private static final String TYPE_KEY = "type";
  private static final String TENANT_KEY = "tenant";
  private static final String MARC_BIB_RECORD_TYPE = "marc-bib";

  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final MappingMetadataCache mappingMetadataCache;

  public MarcBibUpdateKafkaHandler(InstanceUpdateDelegate instanceUpdateDelegate, MappingMetadataCache mappingMetadataCache) {
    this.instanceUpdateDelegate = instanceUpdateDelegate;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      @SuppressWarnings("unchecked")
      HashMap<String, String> payloadMap = OBJECT_MAPPER.readValue(record.value(), HashMap.class);
      Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
      String jobId = payloadMap.get(JOB_ID);
      String type = payloadMap.get(TYPE_KEY);
      LOGGER.info("Event payload has been received with event type: {} by jobId: {}", type, jobId);

      if (isEmpty(payloadMap.get(RECORD_KEY))) {
        String message = String.format("Event message does not contain required data to update Instance by jobId: '%s'", jobId);
        LOGGER.error(message);
        return Future.failedFuture(message);
      }
      Context context = EventHandlingUtil.constructContext(payloadMap.get(TENANT_KEY), headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER));
      Record marcBibRecord = new JsonObject(payloadMap.get(RECORD_KEY)).mapTo(Record.class);

      mappingMetadataCache.getByRecordType(jobId, context, MARC_BIB_RECORD_TYPE)
        .map(metadataOptional -> metadataOptional.orElseThrow(() ->
          new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobId))))
        .onSuccess(mappingMetadataDto -> ensureEventPayloadWithMappingMetadata(payloadMap, mappingMetadataDto))
        .compose(v -> instanceUpdateDelegate.handle(payloadMap, marcBibRecord, context))
        .onComplete(ar -> {
          if (ar.succeeded()) {
            promise.complete(record.key());
          } else {
            LOGGER.error("Failed to update instance by jobId {}:{}", jobId, ar.cause());
            promise.fail(ar.cause());
          }
        });
      return promise.future();
    } catch (Exception e) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", record.topic()), e);
      return Future.failedFuture(e);
    }
  }
  private void ensureEventPayloadWithMappingMetadata(HashMap<String, String> eventPayload, MappingMetadataDto mappingMetadataDto) {
    eventPayload.put(MAPPING_RULES_KEY, mappingMetadataDto.getMappingRules());
    eventPayload.put(MAPPING_PARAMS_KEY, mappingMetadataDto.getMappingParams());
  }

}
