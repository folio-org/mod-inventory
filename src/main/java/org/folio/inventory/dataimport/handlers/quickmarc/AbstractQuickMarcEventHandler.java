package org.folio.inventory.dataimport.handlers.quickmarc;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.Map;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Record;

public abstract class AbstractQuickMarcEventHandler<T> {

  private static final Logger LOGGER = LogManager.getLogger(AbstractQuickMarcEventHandler.class);

  public static final String RECORD_TYPE_KEY = "RECORD_TYPE";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";

  public Future<T> handle(Map<String, String> eventPayload) {
    Promise<T> promise = Promise.promise();
    try {
      if (isValidPayload(eventPayload)) {
        String message = "Event does not contain required data to update Record";
        LOGGER.error(message);
        promise.fail(new EventProcessingException(message));
        return promise.future();
      }

      Record marcRecord = new JsonObject(eventPayload.get(eventPayload.get(RECORD_TYPE_KEY))).mapTo(Record.class);
      updateEntity(eventPayload, marcRecord, promise);
    } catch (Exception e) {
      LOGGER.error("Failed to update entity", e);
      promise.fail(e);
    }
    return promise.future();
  }

  protected abstract void updateEntity(Map<String, String> eventPayload, Record marcRecord, Promise<T> handler);

  private boolean isValidPayload(Map<String, String> eventPayload) {
    return eventPayload == null
      || isEmpty(eventPayload.get(RECORD_TYPE_KEY))
      || isEmpty(eventPayload.get(eventPayload.get(RECORD_TYPE_KEY)))
      || isEmpty(eventPayload.get(MAPPING_RULES_KEY))
      || isEmpty(eventPayload.get(MAPPING_PARAMS_KEY));
  }

}
