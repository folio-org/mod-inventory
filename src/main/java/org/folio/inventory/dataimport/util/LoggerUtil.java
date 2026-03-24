package org.folio.inventory.dataimport.util;

import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;
import java.util.Map;

public class LoggerUtil {
  public static final String INCOMING_RECORD_ID = "INCOMING_RECORD_ID";
  public static final String RECORD_ID_HEADER = "recordId";

  public static String extractRecordId(DataImportEventPayload eventPayload) {
    if (eventPayload == null || eventPayload.getContext() == null) {
      return "";
    }
    return eventPayload.getContext().getOrDefault(RECORD_ID_HEADER, "");
  }

  public static void logParametersEventHandler(Logger LOGGER, DataImportEventPayload dataImportEventPayload) {
    HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
    LOGGER.debug("handle:: parameters jobExecutionId: {} recordId: {} eventType: {} and incomingRecordId: {} ",
      dataImportEventPayload.getJobExecutionId(), extractRecordId(dataImportEventPayload),
      dataImportEventPayload.getEventType(),
      payloadContext != null ? payloadContext.get(INCOMING_RECORD_ID) : null);
    LOGGER.trace("handle:: parameter jobExecutionId: {} recordId: {} dataImportEventPayload: {}",
      dataImportEventPayload.getJobExecutionId(), extractRecordId(dataImportEventPayload), dataImportEventPayload);
  }

  public static void logParametersUpdateDelegate(Logger LOGGER, Map<String, String> eventPayload, Record marcRecord, Context context) {
    LOGGER.trace("handle:: parameters eventPayload: {} , marcRecord: {} , context: {}", eventPayload, marcRecord, context);
  }
}
