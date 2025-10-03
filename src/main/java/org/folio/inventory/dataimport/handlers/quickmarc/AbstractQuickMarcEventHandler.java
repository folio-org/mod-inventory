package org.folio.inventory.dataimport.handlers.quickmarc;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import io.vertx.core.json.Json;
import java.util.Map;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Record;

public abstract class AbstractQuickMarcEventHandler<T> {

  private static final Logger LOGGER = LogManager.getLogger(AbstractQuickMarcEventHandler.class);

  public static final String RECORD_TYPE_KEY = "RECORD_TYPE";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";

  public Future<T> handle(Map<String, Object> eventPayload) {
    Promise<T> promise = Promise.promise();
    try {
      if (isValidPayload(eventPayload)) {
        String message = "Event does not contain required data to update Record";
        LOGGER.error(message);
        promise.fail(new EventProcessingException(message));
        return promise.future();
      }
      var recordType = eventPayload.get(RECORD_TYPE_KEY).toString();
      Map<String, Object> recordTypeMap = ((Map<String, Object>) eventPayload.get(recordType));
      Record marcRecord = Json.CODEC.fromValue(recordTypeMap, Record.class);
      updateEntity(eventPayload, marcRecord, promise);
    } catch (Exception e) {
      LOGGER.error("Failed to update entity", e);
      promise.fail(e);
    }
    return promise.future();
  }

  protected abstract void updateEntity(Map<String, Object> eventPayload, Record marcRecord, Promise<T> handler);

  private boolean isValidPayload(Map<String, Object> eventPayload) {
    return eventPayload == null
      || isEmpty(eventPayload.get(RECORD_TYPE_KEY).toString())
      || isEmpty(eventPayload.get(eventPayload.get(RECORD_TYPE_KEY).toString()).toString())
      || isEmpty(eventPayload.get(MAPPING_RULES_KEY).toString())
      || isEmpty(eventPayload.get(MAPPING_PARAMS_KEY).toString());
  }

}
