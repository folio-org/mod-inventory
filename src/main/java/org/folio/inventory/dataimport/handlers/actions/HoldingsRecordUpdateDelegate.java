package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;

import static java.lang.String.format;

public class HoldingsRecordUpdateDelegate {
  private static final Logger LOGGER = LogManager.getLogger(HoldingsRecordUpdateDelegate.class);
  private Storage storage;

  public HoldingsRecordUpdateDelegate(Storage storage) {
    this.storage = storage;
  }

  public Future<HoldingsRecord> handle(HoldingsRecord mappedHoldingsRecord, String existingHoldingsRecordId, Context context) {
    try {
      HoldingsRecordCollection holdingsRecordCollection = storage.getHoldingsRecordCollection(context);
      return getRecordById(existingHoldingsRecordId, holdingsRecordCollection)
        .compose(existingRecord -> mergeRecords(existingRecord, mappedHoldingsRecord))
        .compose(updatedRecord -> updateRecordInStorage(updatedRecord, holdingsRecordCollection));
    } catch (Exception e) {
      LOGGER.error("Error updating inventory Holdings record", e);
      return Future.failedFuture(e);
    }
  }

  private Future<HoldingsRecord> getRecordById(String holdingsRecordId, HoldingsRecordCollection holdingsRecordCollection) {
    Promise<HoldingsRecord> promise = Promise.promise();
    holdingsRecordCollection.findById(holdingsRecordId, success -> promise.complete(success.getResult()),
      failure -> {
        LOGGER.error(format("Error retrieving Holdings record by id %s - %s, status code %s", holdingsRecordId, failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private Future<HoldingsRecord> mergeRecords(HoldingsRecord existingRecord, HoldingsRecord mappedRecord) {
    try {
      mappedRecord.setId(existingRecord.getId());
      JsonObject existing = JsonObject.mapFrom(existingRecord);
      JsonObject mapped = JsonObject.mapFrom(mappedRecord);
      JsonObject merged = existing.mergeIn(mapped);
      HoldingsRecord mergedHolding = merged.mapTo(HoldingsRecord.class);
      return Future.succeededFuture(mergedHolding);
    } catch (Exception e) {
      LOGGER.error("Error while merging an existing Holdings record to mapped Holdings record", e);
      return Future.failedFuture(e);
    }
  }

  private Future<HoldingsRecord> updateRecordInStorage(HoldingsRecord holding, HoldingsRecordCollection holdingCollection) {
    Promise<HoldingsRecord> promise = Promise.promise();
    holdingCollection.update(holding, success -> promise.complete(holding),
      failure -> {
        LOGGER.error(format("Error updating Holdings record - %s, status code %s", failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }
}
