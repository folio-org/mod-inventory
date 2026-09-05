package org.folio.inventory.dataimport.util;

import java.util.HashMap;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.DataImportHeaders;
import org.folio.inventory.common.Context;
import org.folio.rest.jaxrs.model.Record;

@UtilityClass
public class LoggerUtil {

  public static final String INCOMING_RECORD_ID = "INCOMING_RECORD_ID";

  public static String extractRecordId(DataImportEventPayload eventPayload) {
    if (eventPayload == null || eventPayload.getContext() == null) {
      return "";
    }
    return eventPayload.getContext().getOrDefault(DataImportHeaders.RECORD_ID, "");
  }

  public static void logParametersEventHandler(Logger logger, DataImportEventPayload dataImportEventPayload) {
    HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
    var recordId = extractRecordId(dataImportEventPayload);
    logger.debug("handle:: parameters jobExecutionId: {} recordId: {} eventType: {} and incomingRecordId: {} ",
      dataImportEventPayload.getJobExecutionId(), recordId,
      dataImportEventPayload.getEventType(),
      payloadContext != null ? payloadContext.get(INCOMING_RECORD_ID) : null);
    logger.trace("handle:: parameter jobExecutionId: {} recordId: {} dataImportEventPayload: {}",
      dataImportEventPayload.getJobExecutionId(), recordId, dataImportEventPayload);
  }

  public static void logParametersUpdateDelegate(Logger logger, Map<String, String> eventPayload, Record marcRecord,
                                                 Context context) {
    logger.trace("handle:: parameters eventPayload: {} , marcRecord: {} , context: {}", eventPayload, marcRecord,
      context);
  }
}
