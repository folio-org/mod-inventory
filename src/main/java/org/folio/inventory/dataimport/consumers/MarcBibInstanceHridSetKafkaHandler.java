package org.folio.inventory.dataimport.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isAnyEmpty;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

public class MarcBibInstanceHridSetKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MarcBibInstanceHridSetKafkaHandler.class);
  private static final String MARC_KEY = "MARC";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();

  private InstanceUpdateDelegate instanceUpdateDelegate;

  public MarcBibInstanceHridSetKafkaHandler(InstanceUpdateDelegate instanceUpdateDelegate) {
    this.instanceUpdateDelegate = instanceUpdateDelegate;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      Event event = OBJECT_MAPPER.readValue(record.value(), Event.class);
      HashMap<String, String> eventPayload = OBJECT_MAPPER.readValue(ZIPArchiver.unzip(event.getEventPayload()), HashMap.class);
      LOGGER.info("Event payload has been received with event type: {}", event.getEventType());

      if (isAnyEmpty(eventPayload.get(MARC_KEY), eventPayload.get(MAPPING_RULES_KEY), eventPayload.get(MAPPING_PARAMS_KEY))) {
        String message = "Event payload does not contain required data to update Instance";
        LOGGER.error(message);
        return Future.failedFuture(message);
      }

      Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
      Context context = EventHandlingUtil.constructContext(headersMap.get(OKAPI_TENANT_HEADER), headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER));
      Record marcRecord = new JsonObject(eventPayload.get(MARC.value())).mapTo(Record.class);

      instanceUpdateDelegate.handle(eventPayload, marcRecord, context).onComplete(ar -> {
        if (ar.succeeded()) {
          promise.complete(record.key());
        } else {
          LOGGER.error("Failed to process data import event payload", ar.cause());
          promise.fail(ar.cause());
        }
      });
      return promise.future();
    } catch (Exception e) {
      LOGGER.error("Failed to process data import kafka record from topic {}", e, record.topic());
      return Future.failedFuture(e);
    }
  }
}
