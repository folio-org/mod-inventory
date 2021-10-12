package org.folio.inventory.resources;

import static io.netty.util.internal.StringUtil.COMMA;
import static java.util.concurrent.CompletableFuture.completedFuture;

import static org.folio.inventory.support.CompletableFutures.failedFuture;
import static org.folio.inventory.support.EndpointFailureHandler.doExceptionally;
import static org.folio.inventory.support.http.server.SuccessResponse.noContent;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.HoldingsRecord;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.config.InventoryConfiguration;
import org.folio.inventory.config.InventoryConfigurationImpl;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.server.FailureResponseConsumer;

public class Holdings {

  private static final Logger LOGGER = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  private static final String HRID_UPDATED_ERROR_MSG = "HRID can not be updated";
  private static final String HOLDINGS_NOT_FOUND_ERROR_MSG = "Holdings not found";
  private static final String BLOCKED_FIELDS_UPDATED_ERROR_MSG = "Holdings is controlled by MARC record, "
    + "these fields are blocked and can not be updated: ";

  private static final String INVENTORY_PATH = "/inventory";
  private static final String HOLDINGS_PATH = INVENTORY_PATH + "/holdings";
  private static final String HRID_FIELD = "hrid";
  private static final String ID_FIELD = "id";
  private static final String MARC_SOURCE_ID = "036ee84a-6afd-4c3c-9ad3-4a12ab875f59";

  private final Storage storage;
  private final InventoryConfiguration config;

  public Holdings(final Storage storage) {
    this.storage = storage;
    this.config = new InventoryConfigurationImpl();
  }

  public void register(Router router) {
    router.put(HOLDINGS_PATH + "*").handler(BodyHandler.create());
    router.put(HOLDINGS_PATH + "/:id").handler(this::update);
  }

  private CompletableFuture<HoldingsRecord> refuseWhenInstanceNotFound(HoldingsRecord holdingsRecord) {
    return holdingsRecord == null
      ? failedFuture(new NotFoundException(HOLDINGS_NOT_FOUND_ERROR_MSG))
      : completedFuture(holdingsRecord);
  }

  private CompletableFuture<HoldingsRecord> refuseWhenHridChanged(HoldingsRecord existingHoldings,
                                                                  HoldingsRecord updatedHoldings) {

    return Objects.equals(existingHoldings.getHrid(), updatedHoldings.getHrid())
      ? completedFuture(existingHoldings)
      : failedFuture(new UnprocessableEntityException(HRID_UPDATED_ERROR_MSG, HRID_FIELD, updatedHoldings.getHrid()));
  }

  private void update(RoutingContext rContext) {
    var wContext = new WebContext(rContext);
    var holdingsRequest = rContext.getBodyAsJson();
    var updatedHoldings = holdingsRequest.mapTo(HoldingsRecord.class);
    var holdingsRecordCollection = storage.getHoldingsRecordCollection(wContext);

    completedFuture(updatedHoldings)
      .thenCompose(holdingsRecord -> holdingsRecordCollection.findById(rContext.request().getParam(ID_FIELD)))
      .thenCompose(this::refuseWhenInstanceNotFound)
      .thenCompose(existingHoldings -> refuseWhenBlockedFieldsChanged(existingHoldings, updatedHoldings))
      .thenCompose(existingHoldings -> refuseWhenHridChanged(existingHoldings, updatedHoldings))
      .thenAccept(existingHoldings -> updateHoldings(updatedHoldings, rContext, wContext))
      .exceptionally(doExceptionally(rContext));
  }

  private CompletionStage<HoldingsRecord> refuseWhenBlockedFieldsChanged(HoldingsRecord existingHoldings,
                                                                         HoldingsRecord updatedHoldings) {

    if (isHoldingsControlledByRecord(existingHoldings)
      && areHoldingsBlockedFieldsChanged(existingHoldings, updatedHoldings)) {
      var errorMessage = BLOCKED_FIELDS_UPDATED_ERROR_MSG + StringUtils.join(config.getHoldingsBlockedFields(), COMMA);
      LOGGER.error(errorMessage);
      return failedFuture(new UnprocessableEntityException(errorMessage, null, null));
    }

    return completedFuture(existingHoldings);
  }

  private boolean isHoldingsControlledByRecord(HoldingsRecord holdingsRecord) {
    return MARC_SOURCE_ID.equals(holdingsRecord.getSourceId());
  }

  private boolean areHoldingsBlockedFieldsChanged(HoldingsRecord existingHoldings, HoldingsRecord updatedHoldings) {
    var existingHoldingsJson = JsonObject.mapFrom(existingHoldings);
    var updatedHoldingsJson = JsonObject.mapFrom(updatedHoldings);

    Map<String, Object> existingBlockedFields = new HashMap<>();
    Map<String, Object> updatedBlockedFields = new HashMap<>();
    for (var blockedFieldName : config.getHoldingsBlockedFields()) {
      existingBlockedFields.put(blockedFieldName, existingHoldingsJson.getValue(blockedFieldName));
      updatedBlockedFields.put(blockedFieldName, updatedHoldingsJson.getValue(blockedFieldName));
    }
    return ObjectUtils.notEqual(existingBlockedFields, updatedBlockedFields);
  }

  private void updateHoldings(HoldingsRecord holdingsRecord, RoutingContext rContext, WebContext wContext) {
    HoldingsRecordCollection instanceCollection = storage.getHoldingsRecordCollection(wContext);
    instanceCollection.update(holdingsRecord,
      voidSuccess -> noContent(rContext.response()),
      FailureResponseConsumer.serverError(rContext.response())
    );
  }
}
