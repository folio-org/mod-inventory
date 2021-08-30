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

public class HoldingUpdateDelegate {
  private static final Logger LOGGER = LogManager.getLogger(HoldingUpdateDelegate.class);
  protected final Storage STORAGE;

  public HoldingUpdateDelegate(Storage storage) {
    this.STORAGE = storage;
  }

  public Future<HoldingsRecord> handle(HoldingsRecord mappedHolding, String existingHoldingsId, Context context) {
    try {
      HoldingsRecordCollection holdingsRecordCollection = STORAGE.getHoldingsRecordCollection(context);
      return getHoldingById(existingHoldingsId, holdingsRecordCollection)
        .compose(existingRecord -> updateHolding(existingRecord, mappedHolding))
        .compose(updatedRecord -> updateHoldingInStorage(updatedRecord, holdingsRecordCollection));
    } catch (Exception e) {
      LOGGER.error("Error updating inventory Holding", e);
      return Future.failedFuture(e);
    }
  }

  private Future<HoldingsRecord> getHoldingById(String holdingId, HoldingsRecordCollection holdingsRecordCollection) {
    Promise<HoldingsRecord> promise = Promise.promise();
    holdingsRecordCollection.findById(holdingId, success -> promise.complete(success.getResult()),
      failure -> {
        LOGGER.error(format("Error retrieving Holding by id %s - %s, status code %s", holdingId, failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private Future<HoldingsRecord> updateHolding(HoldingsRecord existingRecord, HoldingsRecord mappedRecord) {
    try {
      mappedRecord.setId(existingRecord.getId());
      JsonObject existing = JsonObject.mapFrom(existingRecord);
      JsonObject mapped = JsonObject.mapFrom(mappedRecord);
      JsonObject merged = existing.mergeIn(mapped);
      HoldingsRecord mergedHolding = merged.mapTo(HoldingsRecord.class);
      return Future.succeededFuture(mergedHolding);
    } catch (Exception e) {
      LOGGER.error("Error updating Holding", e);
      return Future.failedFuture(e);
    }
  }

  private Future<HoldingsRecord> updateHoldingInStorage(HoldingsRecord holding, HoldingsRecordCollection holdingCollection) {
    Promise<HoldingsRecord> promise = Promise.promise();
    holdingCollection.update(holding, success -> promise.complete(holding),
      failure -> {
        LOGGER.error(format("Error updating Holding - %s, status code %s", failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }
}
