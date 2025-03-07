package org.folio.inventory.dataimport.util;

import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;
import java.util.Map;

public class LoggerUtil {
  public static final String INCOMING_RECORD_ID = "INCOMING_RECORD_ID";
  public static void logParametersEventHandler(Logger LOGGER, DataImportEventPayload dataImportEventPayload) {
    HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
    LOGGER.info("handle:: parameters jobExecutionId: {}, eventType: {} and incomingRecordId: {} ", dataImportEventPayload.getJobExecutionId(), dataImportEventPayload.getEventType(),
      payloadContext != null ? payloadContext.get(INCOMING_RECORD_ID) : null);
    LOGGER.info("handle:: parameter dataImportEventPayload: {}", dataImportEventPayload);
  }

  public static void logParametersUpdateDelegate(Logger LOGGER, Map<String, String> eventPayload, Record marcRecord, Context context) {
    LOGGER.info("handle:: parameters eventPayload: {} , marcRecord: {} , context: {}", eventPayload, marcRecord, context);
  }
}
