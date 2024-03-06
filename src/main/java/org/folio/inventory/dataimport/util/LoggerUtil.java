package org.folio.inventory.dataimport.util;

import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.rest.jaxrs.model.Record;

import java.util.Map;

public class LoggerUtil {
  public static final String INCOMING_RECORD_ID = "INCOMING_RECORD_ID";
  public static void logParametersEventHandler(Logger LOGGER, DataImportEventPayload dataImportEventPayload) {
    LOGGER.debug("handle:: parameters jobExecutionId: {}, eventType: {} and incomingRecordId: {} ", dataImportEventPayload.getJobExecutionId(), dataImportEventPayload.getEventType(), dataImportEventPayload.getContext().get(INCOMING_RECORD_ID));
    LOGGER.trace("handle:: parameter dataImportEventPayload: {}", dataImportEventPayload);
  }

  public static void logParametersUpdateDelegate(Logger LOGGER, Map<String, String> eventPayload, Record marcRecord, Context context) {
    LOGGER.trace("handle:: parameters eventPayload: {} , marcRecord: {} , context: {}", eventPayload, marcRecord, context);
  }
}
