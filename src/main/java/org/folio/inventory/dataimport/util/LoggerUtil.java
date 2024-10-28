package org.folio.inventory.dataimport.util;

import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;
import java.util.Map;

public class LoggerUtil {
  public static final String INCOMING_RECORD_ID = "INCOMING_RECORD_ID";
  private static final String RECORD_ID_HEADER = "recordId";
  public static final String LOG_START_TEMPLATE = "{}, jobExecutionId: '{}', incomingRecordId: '{}'. additionalLogs::";
  public static final String LOG_END_TEMPLATE = "{}, jobExecutionId: '{}', incomingRecordId: '{}', duration: {} ms. additionalLogs::";

  public static void logParametersEventHandler(Logger LOGGER, DataImportEventPayload dataImportEventPayload) {
    HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
    LOGGER.debug("handle:: parameters jobExecutionId: {}, eventType: {} and incomingRecordId: {} ", dataImportEventPayload.getJobExecutionId(), dataImportEventPayload.getEventType(),
      payloadContext != null ? payloadContext.get(INCOMING_RECORD_ID) : null);
    LOGGER.trace("handle:: parameter dataImportEventPayload: {}", dataImportEventPayload);
  }

  public static void logParametersUpdateDelegate(Logger LOGGER, Map<String, String> eventPayload, Record marcRecord, Context context) {
    LOGGER.trace("handle:: parameters eventPayload: {} , marcRecord: {} , context: {}", eventPayload, marcRecord, context);
  }

  public static void logStart(String msg, DataImportEventPayload dataImportEventPayload, Logger logger) {
    String incomingRecordId = dataImportEventPayload.getContext() != null
      ? dataImportEventPayload.getContext().get(RECORD_ID_HEADER) : null;
    logger.info(LOG_START_TEMPLATE, msg, dataImportEventPayload.getJobExecutionId(), incomingRecordId);
  }

  public static void logStart(String msg, Context context, Logger logger) {
    String jobExecutionId = null;
    String recordId = null;

    if (context instanceof EventHandlingUtil.ExtendedContext extendedContext) {
      jobExecutionId = extendedContext.getJobExecutionId();
      recordId = extendedContext.getRecordId();
    }

    logger.info(LOG_START_TEMPLATE, msg, jobExecutionId, recordId);
  }

  public static void logEnd(String msg, DataImportEventPayload dataImportEventPayload, long startTime, Logger logger) {
    long endTime = System.currentTimeMillis();
    String incomingRecordId = dataImportEventPayload.getContext() != null
      ? dataImportEventPayload.getContext().get(RECORD_ID_HEADER) : null;

    logger.info(LOG_END_TEMPLATE, msg, dataImportEventPayload.getJobExecutionId(), incomingRecordId, endTime - startTime);
  }

  public static void logEnd(String msg, Context context, long startTime, Logger logger) {
    long endTime = System.currentTimeMillis();
    String jobExecutionId = null;
    String recordId = null;

    if (context instanceof EventHandlingUtil.ExtendedContext extendedContext) {
      jobExecutionId = extendedContext.getJobExecutionId();
      recordId = extendedContext.getRecordId();
    }

    logger.info(LOG_END_TEMPLATE, msg, jobExecutionId, recordId, endTime - startTime);
  }

}
