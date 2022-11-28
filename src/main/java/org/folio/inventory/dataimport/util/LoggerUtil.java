package org.folio.inventory.dataimport.util;

import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.rest.jaxrs.model.Record;

import java.util.Map;

public class LoggerUtil {
  public static void logParametersEventHandler(Logger LOGGER, DataImportEventPayload dataImportEventPayload) {
    LOGGER.debug("handle:: parameters jobExecutionId: {} and eventType: {} ", dataImportEventPayload.getJobExecutionId(), dataImportEventPayload.getEventType());
    LOGGER.trace("handle:: parameter dataImportEventPayload: {}", dataImportEventPayload);
  }

  public static void logParametersUpdateDelegate(Logger LOGGER, Map<String, String> eventPayload, Record marcRecord, Context context) {
    LOGGER.trace("handle:: parameters eventPayload: {} , marcRecord: {} , context: {}", eventPayload, marcRecord, context);
  }
}
