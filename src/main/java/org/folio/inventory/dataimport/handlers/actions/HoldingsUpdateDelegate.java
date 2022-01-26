package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;

import java.util.Map;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.Holdings;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

public class HoldingsUpdateDelegate {

  private static final Logger LOGGER = LogManager.getLogger(HoldingsUpdateDelegate.class);

  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String QM_RELATED_RECORD_VERSION_KEY = "RELATED_RECORD_VERSION";
  private static final String MARC_FORMAT = "MARC_HOLDINGS";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));

  private final Storage storage;

  public HoldingsUpdateDelegate(Storage storage) {
    this.storage = storage;
  }

  public Future<HoldingsRecord> handle(Map<String, String> eventPayload, Record marcRecord, Context context) {
    try {
      JsonObject mappingRules = new JsonObject(eventPayload.get(MAPPING_RULES_KEY));
      MappingParameters mappingParameters =
        new JsonObject(eventPayload.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);

      JsonObject parsedRecord = retrieveParsedContent(marcRecord.getParsedRecord());
      String holdingsId = marcRecord.getExternalIdsHolder().getHoldingsId();
      RecordMapper<Holdings> recordMapper = RecordMapperBuilder.buildMapper(MARC_FORMAT);
      var mappedHoldings = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      HoldingsRecordCollection holdingsRecordCollection = storage.getHoldingsRecordCollection(context);

      return getHoldingsRecordById(holdingsId, holdingsRecordCollection)
        .onSuccess(existingHoldingsRecord -> fillVersion(existingHoldingsRecord, eventPayload))
        .compose(existingHoldingsRecord -> mergeRecords(existingHoldingsRecord, mappedHoldings))
        .compose(updatedHoldingsRecord -> updateHoldingsRecordAndRetryIfOLErrorExist(updatedHoldingsRecord, holdingsRecordCollection,
          marcRecord, eventPayload, context));
    } catch (Exception e) {
      eventPayload.remove(CURRENT_RETRY_NUMBER);
      LOGGER.error("Error updating inventory holdings", e);
      return Future.failedFuture(e);
    }
  }

  private JsonObject retrieveParsedContent(ParsedRecord parsedRecord) {
    return parsedRecord.getContent() instanceof String
      ? new JsonObject(parsedRecord.getContent().toString())
      : JsonObject.mapFrom(parsedRecord.getContent());
  }

  private Future<HoldingsRecord> getHoldingsRecordById(String holdingsId,
                                                       HoldingsRecordCollection holdingsRecordCollection) {
    Promise<HoldingsRecord> promise = Promise.promise();
    holdingsRecordCollection.findById(holdingsId, success -> promise.complete(success.getResult()),
      failure -> {
        LOGGER.error(format("Error retrieving Holdings by id %s - %s, status code %s", holdingsId, failure.getReason(),
          failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private void fillVersion(HoldingsRecord existingHoldingsRecord, Map<String, String> eventPayload) {
    if (eventPayload.containsKey(QM_RELATED_RECORD_VERSION_KEY)) {
      existingHoldingsRecord.setVersion(Integer.parseInt(eventPayload.get(QM_RELATED_RECORD_VERSION_KEY)));
    }
  }

  private Future<HoldingsRecord> mergeRecords(HoldingsRecord existingRecord, Holdings mappedRecord) {
    try {
      mappedRecord.setId(existingRecord.getId());
      JsonObject existing = JsonObject.mapFrom(existingRecord);
      JsonObject mapped = JsonObject.mapFrom(mappedRecord);
      JsonObject merged = existing.mergeIn(mapped);
      HoldingsRecord mergedHoldingsRecord = merged.mapTo(HoldingsRecord.class);
      return Future.succeededFuture(mergedHoldingsRecord);
    } catch (Exception e) {
      LOGGER.error("Error updating holdings", e);
      return Future.failedFuture(e);
    }
  }

  private Future<HoldingsRecord> updateHoldingsRecordAndRetryIfOLErrorExist(HoldingsRecord holdingsRecord, HoldingsRecordCollection holdingsRecordCollection,
                                                                            Record marcRecord, Map<String, String> eventPayload, Context context) {
    Promise<HoldingsRecord> promise = Promise.promise();
    holdingsRecordCollection.update(holdingsRecord, success -> {
        eventPayload.remove(CURRENT_RETRY_NUMBER);
        promise.complete(holdingsRecord);
      },
      failure -> {
        if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
          processOLError(eventPayload, marcRecord, context, promise, failure);
        } else {
          eventPayload.remove(CURRENT_RETRY_NUMBER);
          LOGGER.error(format("Error updating Holdings - %s, status code %s", failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  private void processOLError(Map<String, String> eventPayload, Record marcRecord, Context context, Promise<HoldingsRecord> promise, Failure failure) {
    int currentRetryNumber = eventPayload.get(CURRENT_RETRY_NUMBER)  == null ? 0 : Integer.parseInt(eventPayload.get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      eventPayload.put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("Error updating Holding - {}, status code {}. Retry HoldingsUpdateDelegate handler...", failure.getReason(), failure.getStatusCode());
      handle(eventPayload, marcRecord, context).onComplete(res -> {
        if (res.succeeded()) {
          promise.complete();
        } else {
          promise.fail(res.cause());
        }
      });
    } else {
      eventPayload.remove(CURRENT_RETRY_NUMBER);
      LOGGER.error("Current retry number {} exceeded or equal given number {} for the Holding update", MAX_RETRIES_COUNT, currentRetryNumber);
      promise.fail(format("Current retry number %s exceeded or equal given number %s for the Holding update", MAX_RETRIES_COUNT, currentRetryNumber));
    }
  }
}
