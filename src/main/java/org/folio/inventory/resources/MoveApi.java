package org.folio.inventory.resources;

import static java.lang.String.format;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.resources.UpdateOwnershipApi.TENANT_NOT_IN_CONSORTIA;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.MoveApiUtil.createHoldingsRecordsFetchClient;
import static org.folio.inventory.support.MoveApiUtil.createHoldingsStorageClient;
import static org.folio.inventory.support.MoveApiUtil.createHttpClient;
import static org.folio.inventory.support.MoveApiUtil.createItemStorageClient;
import static org.folio.inventory.support.MoveApiUtil.createItemsFetchClient;
import static org.folio.inventory.support.MoveApiUtil.respond;
import static org.folio.inventory.support.http.server.JsonResponse.unprocessableEntity;
import static org.folio.inventory.validation.MoveValidator.holdingsMoveHasRequiredFields;
import static org.folio.inventory.validation.MoveValidator.itemsMoveHasRequiredFields;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.exceptions.BadRequestException;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.MoveApiUtil;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.support.http.server.ValidationError;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class MoveApi extends AbstractInventoryResource {
  private static final Logger LOGGER = LogManager.getLogger(MethodHandles.lookup().lookupClass());
  public static final String TO_HOLDINGS_RECORD_ID = "toHoldingsRecordId";
  public static final String TO_INSTANCE_ID = "toInstanceId";
  public static final String ITEM_IDS = "itemIds";
  public static final String HOLDINGS_RECORD_IDS = "holdingsRecordIds";
  private final ConsortiumService consortiumService;
  private static final String INSTANCE_NOT_FOUND = "Instance with id=%s not found";

  public MoveApi(final Storage storage, final HttpClient client, ConsortiumService consortiumService) {
    super(storage, client);
    this.consortiumService = consortiumService;
  }

  @Override
  public void register(Router router) {
    router.post("/inventory/holdings*")
      .handler(BodyHandler.create());
    router.post("/inventory/items/move")
      .handler(this::moveItems);
    router.post("/inventory/holdings/move")
      .handler(this::moveHoldings);
  }

  private void moveItems(RoutingContext routingContext) {
    final var context = new WebContext(routingContext);
    final var itemsMoveJsonRequest = routingContext.body().asJsonObject();

    final var validationError = itemsMoveHasRequiredFields(itemsMoveJsonRequest);

    if (validationError.isPresent()) {
      unprocessableEntity(routingContext.response(), validationError.get());
      return;
    }

    final var toHoldingsRecordId = itemsMoveJsonRequest.getString(TO_HOLDINGS_RECORD_ID);
    final var itemIdsToUpdate = toListOfStrings(itemsMoveJsonRequest, ITEM_IDS);

    storage.getHoldingsRecordCollection(context)
      .findById(toHoldingsRecordId)
      .thenAccept(holding -> {
        if (holding != null) {
          try {
            final var itemsStorageClient = createItemStorageClient(createHttpClient(client, routingContext, context), context);
            final var itemsFetchClient = createItemsFetchClient(itemsStorageClient);

            itemsFetchClient.find(itemIdsToUpdate, MoveApiUtil::fetchByIdCql)
              .thenAccept(jsons -> {
                var itemsToUpdate = updateItemFields(toHoldingsRecordId, jsons);
                updateItems(routingContext, context, itemIdsToUpdate, itemsToUpdate);
              })
              .exceptionally(e -> {
                ServerErrorResponse.internalError(routingContext.response(), e);
                return null;
              });
          } catch (Exception e) {
            ServerErrorResponse.internalError(routingContext.response(), e);
          }
        } else {
          unprocessableEntity(routingContext.response(),
              format("Holding with id=%s not found", toHoldingsRecordId));
        }
      })
    .exceptionally(e -> {
      ServerErrorResponse.internalError(routingContext.response(), e);
      return null;
    });
  }

  private void moveHoldings(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);
    JsonObject holdingsMoveJsonRequest = routingContext.body().asJsonObject();

    Optional<ValidationError> validationError = holdingsMoveHasRequiredFields(holdingsMoveJsonRequest);
    if (validationError.isPresent()) {
      unprocessableEntity(routingContext.response(), validationError.get());
      return;
    }
    String toInstanceId = holdingsMoveJsonRequest.getString(TO_INSTANCE_ID);
    List<String> holdingsRecordsIdsToUpdate = toListOfStrings(holdingsMoveJsonRequest.getJsonArray(HOLDINGS_RECORD_IDS));
    storage.getInstanceCollection(context)
      .findById(toInstanceId)
      .thenCompose(instance -> {
        if (instance != null) {
          return CompletableFuture.completedFuture(instance);
        }
        // Instance not found → check central tenant
        return consortiumService.getConsortiumConfiguration(context)
          .toCompletionStage()
          .toCompletableFuture()
          .thenCompose(consortiumConfig -> {
            if (consortiumConfig.isPresent()) {
              Context centralTenantContext = constructContext(
                consortiumConfig.get().getCentralTenantId(), context.getToken(), context.getOkapiLocation(), context.getUserId(), context.getRequestId()
              );

              return storage.getInstanceCollection(centralTenantContext)
                .findById(toInstanceId)
                .thenApply(sharedInstance -> {
                  if (sharedInstance == null) {
                    LOGGER.warn(format(INSTANCE_NOT_FOUND, toInstanceId));
                    throw new BadRequestException(format(INSTANCE_NOT_FOUND, toInstanceId));
                  }
                  LOGGER.info("shared instance with id={} found in central tenant", sharedInstance.getId());
                  return sharedInstance;
                });
            } else {
              LOGGER.warn(format(TENANT_NOT_IN_CONSORTIA, context.getTenantId()));
              throw new BadRequestException(format(INSTANCE_NOT_FOUND, toInstanceId));
            }
          });
      })
      .thenAccept(instance -> {
        try {
          CollectionResourceClient holdingsStorageClient = createHoldingsStorageClient(
            createHttpClient(client, routingContext, context), context);
          MultipleRecordsFetchClient holdingsRecordFetchClient = createHoldingsRecordsFetchClient(holdingsStorageClient);

          holdingsRecordFetchClient.find(holdingsRecordsIdsToUpdate, MoveApiUtil::fetchByIdCql)
            .thenAccept(jsons -> {
              List<HoldingsRecord> holdingsRecordsToUpdate = updateInstanceIdForHoldings(toInstanceId, jsons);
              updateHoldings(routingContext, context, holdingsRecordsIdsToUpdate, holdingsRecordsToUpdate);
            })
            .exceptionally(e -> {
              ServerErrorResponse.internalError(routingContext.response(), e);
              return null;
            });
        } catch (Exception e) {
          ServerErrorResponse.internalError(routingContext.response(), e);
        }
      })
      .exceptionally(e -> {
        if (e.getCause() instanceof BadRequestException) {
          unprocessableEntity(routingContext.response(), e.getCause().getMessage());
        } else {
          ServerErrorResponse.internalError(routingContext.response(), e);
        }
        return null;
      });
  }

  /**
   * Updates the holdingId and sets the order field to null (storage will recalculate order if it doesn't exist)
   * for each item.
   *
   * @param toHoldingsRecordId the id of the holdings record to which items will be moved
   * @param jsons the list of items in JSON format to be updated
   * @return a list of Item objects with updated holdingId and order fields
   */
  private List<Item> updateItemFields(String toHoldingsRecordId, List<JsonObject> jsons) {
    return jsons.stream()
      .map(ItemUtil::fromStoredItemRepresentation)
      .map(item -> item.withHoldingId(toHoldingsRecordId))
      .map(item -> item.withOrder(null))
      .toList();
  }

  private void updateItems(RoutingContext routingContext, WebContext context, List<String> idsToUpdate, List<Item> itemsToUpdate) {
    ItemCollection storageItemCollection = storage.getItemCollection(context);

    List<CompletableFuture<Item>> updates = itemsToUpdate.stream()
      .map(storageItemCollection::update)
      .toList();

    CompletableFuture.allOf(updates.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> updates.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .map(Item::getId)
        .toList())
      .thenAccept(updatedIds -> respond(routingContext, idsToUpdate, updatedIds));
  }

  private List<HoldingsRecord> updateInstanceIdForHoldings(String toInstanceId, List<JsonObject> jsons) {
    jsons.forEach(MoveApiUtil::removeExtraRedundantFields);

    return jsons.stream()
      .map(json -> json.mapTo(HoldingsRecord.class))
      .map(holding -> holding.withInstanceId(toInstanceId))
      .toList();
  }

  private void updateHoldings(RoutingContext routingContext, WebContext context, List<String> idsToUpdate,
      List<HoldingsRecord> holdingsToUpdate) {
    HoldingsRecordCollection storageHoldingsRecordsCollection = storage.getHoldingsRecordCollection(context);

    List<CompletableFuture<HoldingsRecord>> updateFutures = holdingsToUpdate.stream()
      .map(storageHoldingsRecordsCollection::update)
      .toList();

    CompletableFuture.allOf(updateFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> updateFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .map(HoldingsRecord::getId)
        .toList())
      .thenAccept(updatedIds -> respond(routingContext, idsToUpdate, updatedIds));
  }
}
