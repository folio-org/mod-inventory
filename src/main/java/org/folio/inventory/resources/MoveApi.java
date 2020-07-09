package org.folio.inventory.resources;

import static java.util.stream.Collectors.toList;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.http.server.JsonResponse.unprocessableEntity;
import static org.folio.inventory.validation.MoveValidator.holdingsMoveHasRequiredFields;
import static org.folio.inventory.validation.MoveValidator.itemsMoveHasRequiredFields;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.commons.collections15.ListUtils;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.support.http.server.ValidationError;

import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class MoveApi extends AbstractInventoryResource {

  public static final String TO_HOLDINGS_RECORD_ID = "toHoldingsRecordId";
  public static final String TO_INSTANCE_ID = "toInstanceId";
  public static final String ITEM_IDS = "itemIds";
  public static final String HOLDINGS_RECORD_IDS = "holdingsRecordIds";
  public static final String ITEM_STORAGE = "/item-storage/items";
  public static final String ITEMS_PROPERTY = "items";
  public static final String HOLDINGS_RECORDS_PROPERTY = "holdingsRecords";
  public static final String HOLDINGS_STORAGE = "/holdings-storage/holdings";

  public MoveApi(final Storage storage, final HttpClient client) {
    super(storage, client);
  }

  @Override
  public void register(Router router) {
    router.post("/inventory/items/move")
      .handler(this::moveItems);
    router.post("/inventory/holdings*")
      .handler(BodyHandler.create());
    router.post("/inventory/holdings/move")
      .handler(this::moveHoldings);
  }

  private void moveItems(RoutingContext routingContext) {

    WebContext context = new WebContext(routingContext);
    JsonObject itemsMoveJsonRequest = routingContext.getBodyAsJson();

    Optional<ValidationError> validationError = itemsMoveHasRequiredFields(itemsMoveJsonRequest);
    if (validationError.isPresent()) {
      unprocessableEntity(routingContext.response(), validationError.get());
      return;
    }
    String toHoldingsRecordId = itemsMoveJsonRequest.getString(TO_HOLDINGS_RECORD_ID);
    List<String> itemIdsToUpdate = toListOfStrings(itemsMoveJsonRequest.getJsonArray(ITEM_IDS));
    storage.getHoldingsRecordCollection(context)
      .findById(toHoldingsRecordId)
      .thenAccept(holding -> {
        if (Objects.nonNull(holding)) {
          try {
            OkapiHttpClient okapiClient = createHttpClient(routingContext, context);
            CollectionResourceClient itemsStorageClient = createStorageClient(okapiClient, context, ITEM_STORAGE);
            MultipleRecordsFetchClient itemsFetchClient = createFetchClient(itemsStorageClient, ITEMS_PROPERTY);

            itemsFetchClient.find(itemIdsToUpdate, this::fetchByIdCql)
              .thenAccept(jsons -> {
                List<Item> itemsToUpdate = prepareItemsForUpdate(toHoldingsRecordId, jsons);
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
          JsonResponse.unprocessableEntity(routingContext.response(),
              String.format("Holding with id=%s not found", toHoldingsRecordId));
        }
      });

  }

  private List<Item> updateHoldingsRecordIdForItems(String toHoldingsRecordId, List<JsonObject> jsons) {
    return jsons.stream()
      .map(ItemUtil::fromStoredItemRepresentation)
      .map(item -> item.withHoldingId(toHoldingsRecordId))
      .collect(toList());
  }

  private void updateItems(RoutingContext routingContext, WebContext context, List<String> idsToUpdate, List<Item> itemsToUpdate) {
    ItemCollection storageItemCollection = storage.getItemCollection(context);

    List<CompletableFuture<Item>> updates = itemsToUpdate.stream()
      .map(storageItemCollection::update)
      .collect(Collectors.toList());

    CompletableFuture.allOf(updates.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> updates.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .map(Item::getId)
        .collect(toList()))
      .thenAccept(updatedIds -> respond(routingContext, idsToUpdate, updatedIds));
  }

  private void moveHoldings(RoutingContext routingContext) {

    WebContext context = new WebContext(routingContext);
    JsonObject holdingsMoveJsonRequest = routingContext.getBodyAsJson();

    Optional<ValidationError> validationError = holdingsMoveHasRequiredFields(holdingsMoveJsonRequest);
    if (validationError.isPresent()) {
      unprocessableEntity(routingContext.response(), validationError.get());
      return;
    }
    String toInstanceId = holdingsMoveJsonRequest.getString(TO_INSTANCE_ID);
    List<String> holdingsRecordsIdsToUpdate = toListOfStrings(holdingsMoveJsonRequest.getJsonArray(HOLDINGS_RECORD_IDS));
    storage.getInstanceCollection(context)
      .findById(toInstanceId)
      .thenAccept(instance -> {
        if (Objects.nonNull(instance)) {
          try {
            OkapiHttpClient okapiClient = createHttpClient(routingContext, context);
            CollectionResourceClient holdingsStorageClient = createStorageClient(okapiClient, context, HOLDINGS_STORAGE);
            MultipleRecordsFetchClient holdingsRecordFetchClient = createFetchClient(holdingsStorageClient,
                HOLDINGS_RECORDS_PROPERTY);

            holdingsRecordFetchClient.find(holdingsRecordsIdsToUpdate, this::fetchByIdCql)
              .thenAccept(jsons -> {
                List<HoldingsRecord> holdingsRecordsToUpdate = prepareHoldingsRecordsForUpdate(toInstanceId, jsons);
                updateHoldings(routingContext, context, holdingsRecordsIdsToUpdate, holdingsRecordsToUpdate);
              })
              .exceptionally(e -> {
                ServerErrorResponse.internalError(routingContext.response(), e);
                return null;
              });
          } catch (Exception e) {
            ServerErrorResponse.internalError(routingContext.response(), e);
          }
        } else {
          JsonResponse.unprocessableEntity(routingContext.response(), String.format("Instance with id=%s not found", toInstanceId));
        }
      });

  }

  private List<HoldingsRecord> updateInstanceIdForHoldingsRecords(String toInstanceId, List<JsonObject> jsons) {
    return jsons.stream()
      .map(json -> json.mapTo(HoldingsRecord.class))
      .map(holding -> holding.withInstanceId(toInstanceId))
      .collect(toList());
  }

  private void updateHoldings(RoutingContext routingContext, WebContext context, List<String> idsToUpdate,
      List<HoldingsRecord> holdingsToUpdate) {
    HoldingsRecordCollection storageHoldingsRecordsCollection = storage.getHoldingsRecordCollection(context);

    List<CompletableFuture<HoldingsRecord>> updateFutures = holdingsToUpdate.stream()
      .map(storageHoldingsRecordsCollection::update)
      .collect(Collectors.toList());

    CompletableFuture.allOf(updates.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> updates.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .map(HoldingsRecord::getId)
        .collect(toList()))
      .thenAccept(updatedIds -> respond(routingContext, idsToUpdate, updatedIds));
  }

  private void respond(RoutingContext routingContext, List<String> itemIdsToUpdate, List<String> updatedItemIds) {
    List<String> nonUpdatedIds = ListUtils.subtract(itemIdsToUpdate, updatedItemIds);
    HttpServerResponse response = routingContext.response();
    if (nonUpdatedIds.isEmpty()) {
      JsonResponse.successWithEmptyBody(response);
    } else {
      JsonResponse.successWithIds(response, nonUpdatedIds);
    }
  }

  private OkapiHttpClient createHttpClient(RoutingContext routingContext, WebContext context) throws MalformedURLException {
    return new OkapiHttpClient(client, context, exception -> ServerErrorResponse.internalError(routingContext.response(),
        String.format("Failed to contact storage module: %s", exception.toString())));
  }

  private CollectionResourceClient createStorageClient(OkapiHttpClient client, WebContext context, String storageUrl)
      throws MalformedURLException {

    return new CollectionResourceClient(client, new URL(context.getOkapiLocation() + storageUrl));
  }

  private CqlQuery fetchByIdCql(List<String> ids) {
    return CqlQuery.exactMatchAny("id", ids);
  }

  private MultipleRecordsFetchClient createFetchClient(CollectionResourceClient client, String propertyName) {
    return MultipleRecordsFetchClient.builder()
      .withCollectionPropertyName(propertyName)
      .withExpectedStatus(200)
      .withCollectionResourceClient(client)
      .build();
  }
}
