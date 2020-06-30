package org.folio.inventory.resources;

import static java.util.stream.Collectors.toList;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.http.server.JsonResponse.unprocessableEntity;
import static org.folio.inventory.validation.ItemsMoveValidator.itemsMoveHasRequiredFields;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.commons.collections15.ListUtils;
import org.folio.inventory.common.WebContext;
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

public class MoveApi extends AbstractInventoryResource {

  public static final String TO_HOLDINGS_RECORD_ID = "toHoldingsRecordId";
  public static final String ITEM_IDS = "itemIds";

  public MoveApi(final Storage storage, final HttpClient client) {
    super(storage, client);
  }

  @Override
  public void register(Router router) {
    router.post("/inventory/items/move")
      .handler(this::moveItems);
  }

  private void moveItems(RoutingContext routingContext) {

    WebContext context = new WebContext(routingContext);
    JsonObject itemsMoveJsonRequest = routingContext.getBodyAsJson();

    Optional<ValidationError> validationError = itemsMoveHasRequiredFields(itemsMoveJsonRequest);
    if (validationError.isPresent()) {
      unprocessableEntity(routingContext.response(), validationError.get());
    } else {
      String toHoldingsRecordId = itemsMoveJsonRequest.getString(TO_HOLDINGS_RECORD_ID);
      List<String> itemIdsToUpdate = toListOfStrings(itemsMoveJsonRequest.getJsonArray(ITEM_IDS));
      storage.getHoldingsRecordCollection(context)
        .findById(toHoldingsRecordId)
        .thenAccept(holding -> {
          if (Objects.nonNull(holding)) {
            try {
              OkapiHttpClient okapiClient = createHttpClient(routingContext, context);
              CollectionResourceClient itemsStorageClient = createItemsStorageClient(okapiClient, context);
              MultipleRecordsFetchClient multipleRecordsFetchClient = createItemsFetchClient(itemsStorageClient);

              multipleRecordsFetchClient.find(itemIdsToUpdate, this::fetchItemsCql)
                .thenAccept(jsons -> {
                  List<Item> itemsToUpdate = jsons.stream()
                    .map(ItemUtil::fromStoredItemRepresentation)
                    .map(item -> item.withHoldingId(toHoldingsRecordId))
                    .collect(toList());
                  updateItems(routingContext, context, itemIdsToUpdate, itemsToUpdate);
                }).exceptionally(e -> {
                  ServerErrorResponse.internalError(routingContext.response(), e);
                  return null;
                });
            } catch (Exception e) {
              ServerErrorResponse.internalError(routingContext.response(), e);
            }
          } else {
            JsonResponse.unprocessableEntity(routingContext.response(), "Holding with id=" + toHoldingsRecordId + " not found");
          }
        });
    }
  }

  private void updateItems(RoutingContext routingContext, WebContext context, List<String> itemIdsToUpdate,
      List<Item> itemsToUpdate) {
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
      .thenAccept(updatedItemIds -> {
        List<String> nonUpdatedIds = ListUtils.subtract(itemIdsToUpdate, updatedItemIds);
        HttpServerResponse response = routingContext.response();
        if (nonUpdatedIds.isEmpty()) {
          JsonResponse.successWithEmptyBody(response);
        } else {
          JsonResponse.successWithIds(response, nonUpdatedIds);
        }
      });
  }

  private OkapiHttpClient createHttpClient(RoutingContext routingContext, WebContext context) throws MalformedURLException {
    return new OkapiHttpClient(client, context, exception -> ServerErrorResponse.internalError(routingContext.response(),
        String.format("Failed to contact storage module: %s", exception.toString())));
  }

  private CollectionResourceClient createItemsStorageClient(OkapiHttpClient client, WebContext context)
      throws MalformedURLException {

    return createCollectionResourceClient(client, context, "/item-storage/items");
  }

  private CqlQuery fetchItemsCql(List<String> itemsIds) {
    return CqlQuery.exactMatchAny("id", itemsIds);
  }

  private CollectionResourceClient createCollectionResourceClient(OkapiHttpClient client, WebContext context, String rootPath)
      throws MalformedURLException {

    return new CollectionResourceClient(client, new URL(context.getOkapiLocation() + rootPath));
  }

  private MultipleRecordsFetchClient createItemsFetchClient(CollectionResourceClient client) {
    return MultipleRecordsFetchClient.builder()
      .withCollectionPropertyName("items")
      .withExpectedStatus(200)
      .withCollectionResourceClient(client)
      .build();
  }
}
