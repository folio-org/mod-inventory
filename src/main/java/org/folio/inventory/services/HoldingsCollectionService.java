package org.folio.inventory.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.processing.exceptions.EventProcessingException;

import java.io.UnsupportedEncodingException;

import static java.lang.String.format;

public class HoldingsCollectionService {
  private static final Logger LOGGER = LogManager.getLogger(HoldingsCollectionService.class);

  private static final String ERROR_HOLDING_MSG = "Error loading inventory holdings for MARC BIB";

  public Future<String> findSourceIdByName(HoldingsRecordsSourceCollection sourceCollection, String name) {
    Promise<String> promise = Promise.promise();
    try {
      sourceCollection.findByCql(format("name=%s", name), PagingParameters.defaults(),
        findResult -> {
          if (findResult.getResult() != null && findResult.getResult().totalRecords == 1) {
            var sourceId = findResult.getResult().records.getFirst().getId();
            promise.complete(sourceId);
          } else {
            promise.fail(new EventProcessingException("No source id found for holdings with name MARC"));
          }
        },
        failure -> {
          LOGGER.error(format(ERROR_HOLDING_MSG + ". StatusCode: %s. Message: %s", failure.getStatusCode(), failure.getReason()));
          promise.fail(new EventProcessingException(failure.getReason()));
        });
    } catch (UnsupportedEncodingException e) {
      LOGGER.error(ERROR_HOLDING_MSG, e);
      promise.fail(e);
    }

    return promise.future();
  }

  public Future<HoldingsRecord> getById(String holdingsId,
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
        var reason = failure.getReason();
        var message = format("Error retrieving Holdings by id %s - %s, status code %s", holdingsId, reason,
          failure.getStatusCode());
        LOGGER.error(message);
        promise.fail(reason);
      });
    return promise.future();
  }

  public Future<HoldingsRecord> update(HoldingsRecord holdingsRecord,
                                       HoldingsRecordCollection holdingsRecordCollection) {
    Promise<HoldingsRecord> promise = Promise.promise();
    holdingsRecordCollection.update(holdingsRecord, success -> promise.complete(holdingsRecord),
      failure -> {
        if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
          promise.fail(new OptimisticLockingException(failure.getReason()));
        } else {
          var reason = failure.getReason();
          var message = format("Error updating Holdings - %s, status code %s", reason, failure.getStatusCode());
          LOGGER.error(message);
          promise.fail(reason);
        }
      });
    return promise.future();
  }

  public Future<String> findInstanceIdByHrid(InstanceCollection instanceCollection, String instanceHrid) {
    Promise<String> promise = Promise.promise();
    try {
      instanceCollection.findByCql(format("hrid==%s", instanceHrid), PagingParameters.defaults(),
        findResult -> {
          if (findResult.getResult() != null && findResult.getResult().totalRecords == 1) {
            var instanceId = findResult.getResult().records.getFirst().getId();
            promise.complete(instanceId);
          }else{
            promise.fail(new EventProcessingException("No instance id found for marc holdings with hrid: " + instanceHrid));
          }
        },
        failure -> {
          LOGGER.error(format(ERROR_HOLDING_MSG + ". StatusCode: %s. Message: %s", failure.getStatusCode(), failure.getReason()));
          promise.fail(new EventProcessingException(failure.getReason()));
        });
    } catch (UnsupportedEncodingException e) {
      LOGGER.error(ERROR_HOLDING_MSG, e);
      promise.fail(e);
    }
    return promise.future();
  }

}
