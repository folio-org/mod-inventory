package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;
import static org.folio.inventory.dataimport.util.HoldingsRecordUtil.populateUpdatedByUserIdIfNeeded;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersUpdateDelegate;

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
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.services.HoldingsCollectionService;
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
  private static final String MARC_FORMAT = "MARC_HOLDINGS";
  private static final String MARC_NAME = "MARC";

  private final Storage storage;
  private final HoldingsCollectionService holdingsCollectionService;

  public HoldingsUpdateDelegate(Storage storage, HoldingsCollectionService holdingsCollectionService) {
    this.storage = storage;
    this.holdingsCollectionService = holdingsCollectionService;
  }

  public Future<HoldingsRecord> handle(Map<String, String> eventPayload, Record marcRecord, Context context) {
    logParametersUpdateDelegate(LOGGER, eventPayload, marcRecord, context);
    try {
      JsonObject mappingRules = new JsonObject(eventPayload.get(MAPPING_RULES_KEY));
      MappingParameters mappingParameters =
        new JsonObject(eventPayload.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);

      JsonObject parsedRecord = retrieveParsedContent(marcRecord.getParsedRecord());
      String holdingsId = marcRecord.getExternalIdsHolder().getHoldingsId();
      LOGGER.info("Holdings update with holdingId: {}", holdingsId);
      RecordMapper<Holdings> recordMapper = RecordMapperBuilder.buildMapper(MARC_FORMAT);
      var mappedHoldings = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      HoldingsRecordCollection holdingsRecordCollection = storage.getHoldingsRecordCollection(context);

      return getHoldingsRecordById(holdingsId, holdingsRecordCollection)
        .compose(existingHoldingsRecord -> findSourceId(context)
          .compose(sourceId -> {
            mappedHoldings.setSourceId(sourceId);
            return mergeRecords(existingHoldingsRecord, mappedHoldings);
          }))
        .map(updatedHoldingsRecord -> populateUpdatedByUserIdIfNeeded(updatedHoldingsRecord, context))
        .compose(updatedHoldingsRecord -> updateHoldingsRecord(updatedHoldingsRecord, holdingsRecordCollection));
    } catch (Exception e) {
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
    holdingsRecordCollection.findById(holdingsId, success -> {
        if (success.getResult() == null) {
          LOGGER.error("Can't find Holdings by id: {} ", holdingsId);
          promise.fail(new NotFoundException(format("Can't find Holdings by id: %s ", holdingsId)));
        } else {
          promise.complete(success.getResult());
        }
      },
      failure -> {
        LOGGER.error(format("Error retrieving Holdings by id %s - %s, status code %s", holdingsId, failure.getReason(),
          failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private Future<HoldingsRecord> mergeRecords(HoldingsRecord existingRecord, Holdings mappedRecord) {
    try {
      mappedRecord.setId(existingRecord.getId());
      JsonObject existing = JsonObject.mapFrom(existingRecord);
      JsonObject mapped = JsonObject.mapFrom(mappedRecord);
      JsonObject merged = existing.mergeIn(mapped);
      HoldingsRecord mergedHoldingsRecord = merged.mapTo(HoldingsRecord.class);
      updateCellNumberFields(mergedHoldingsRecord, mappedRecord);
      return Future.succeededFuture(mergedHoldingsRecord);
    } catch (Exception e) {
      LOGGER.error("Error updating holdings", e);
      return Future.failedFuture(e);
    }
  }

  private void updateCellNumberFields(HoldingsRecord existingRecord, Holdings mappedHoldings) {
    existingRecord.setShelvingTitle(mappedHoldings.getShelvingTitle());
    existingRecord.setCopyNumber(mappedHoldings.getCopyNumber());
    existingRecord.setCallNumberTypeId(mappedHoldings.getCallNumberTypeId());
    existingRecord.setCallNumberPrefix(mappedHoldings.getCallNumberPrefix());
    existingRecord.setCallNumber(mappedHoldings.getCallNumber());
    existingRecord.setCallNumberSuffix(mappedHoldings.getCallNumberSuffix());
  }

  private Future<HoldingsRecord> updateHoldingsRecord(HoldingsRecord holdingsRecord,
                                                      HoldingsRecordCollection holdingsRecordCollection) {
    Promise<HoldingsRecord> promise = Promise.promise();
    holdingsRecordCollection.update(holdingsRecord, success -> promise.complete(holdingsRecord),
      failure -> {
        if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
          promise.fail(new OptimisticLockingException(failure.getReason()));
        } else {
          LOGGER.error(format("Error updating Holdings - %s, status code %s", failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  private Future<String> findSourceId(Context context) {
    var sourceCollection = storage.getHoldingsRecordsSourceCollection(context);
    return holdingsCollectionService.findSourceIdByName(sourceCollection, MARC_NAME);
  }
}
