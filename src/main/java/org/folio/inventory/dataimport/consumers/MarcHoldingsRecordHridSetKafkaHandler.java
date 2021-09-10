package org.folio.inventory.dataimport.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.actions.HoldingsRecordUpdateDelegate;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.mapping.MappingManager;
import org.folio.rest.jaxrs.model.Event;

import java.util.Map;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.MARC_HOLDINGS;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

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
  private static final String CORRELATION_ID_HEADER = "correlationId";
  private static final String HOLDINGS_ID = "holdingsId";
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();

  private final HoldingsRecordUpdateDelegate holdingsRecordUpdateDelegate;
  private final KafkaInternalCache kafkaInternalCache;

  public MarcHoldingsRecordHridSetKafkaHandler(HoldingsRecordUpdateDelegate holdingsRecordUpdateDelegate, KafkaInternalCache kafkaInternalCache) {
    this.holdingsRecordUpdateDelegate = holdingsRecordUpdateDelegate;
    this.kafkaInternalCache = kafkaInternalCache;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      Event event = OBJECT_MAPPER.readValue(record.value(), Event.class);
      if (!kafkaInternalCache.containsByKey(event.getId())) {
        kafkaInternalCache.putToCache(event.getId());
        @SuppressWarnings("unchecked")
        Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
        String correlationId = headersMap.get(CORRELATION_ID_HEADER);
        LOGGER.info(format("Event payload has been received with event type: %s and correlationId: %s", event.getEventType(), correlationId));
        DataImportEventPayload eventPayload = new JsonObject(event.getEventPayload()).mapTo(DataImportEventPayload.class);

        Context context = EventHandlingUtil.constructContext(headersMap.get(OKAPI_TENANT_HEADER), headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER));
        if (!(eventPayload.getContext().containsKey(HOLDINGS_ID) && eventPayload.getContext().containsKey(HOLDINGS.value()) && eventPayload.getContext().containsKey(MARC_HOLDINGS.value()))) {
          throw new IllegalArgumentException(format("The event payload does not contain all the %s, %s, %s", HOLDINGS_ID, HOLDINGS.value(), MARC_HOLDINGS.value()));
        }
        String existingRecordId = eventPayload.getContext().get(HOLDINGS_ID);
        MappingManager.map(eventPayload);
        HoldingsRecord mappedRecord = new JsonObject(eventPayload.getContext().get(HOLDINGS.value())).mapTo(HoldingsRecord.class);
        holdingsRecordUpdateDelegate.handle(mappedRecord, existingRecordId, context).onComplete(ar -> {
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
}
