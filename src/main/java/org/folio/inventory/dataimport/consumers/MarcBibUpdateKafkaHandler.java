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
import org.folio.inventory.domain.dto.InstanceEvent;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Record;

public class MarcBibUpdateKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(MarcBibUpdateKafkaHandler.class);
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingParameters and mapping rules snapshots were not found by jobId '%s'";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();
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
      InstanceEvent instanceEvent = OBJECT_MAPPER.readValue(record.value(), InstanceEvent.class);
      Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
      HashMap<String, String> metaDataPayload = new HashMap<>();

      LOGGER.info("Event payload has been received with event type: {} by jobId: {}", instanceEvent.getType(), instanceEvent.getJobId());

      if (isEmpty(instanceEvent.getRecord()) || !InstanceEvent.EventType.UPDATE.equals(instanceEvent.getType())) {
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
        .onComplete(ar -> {
          if (ar.succeeded()) {
            promise.complete(record.key());
          } else {
            LOGGER.error("Failed to update instance by jobId {}:{}", instanceEvent.getJobId(), ar.cause());
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
