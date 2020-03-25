package org.folio.inventory.services;

import static org.folio.inventory.storage.external.CqlQuery.exactMatch;

import java.util.concurrent.CompletableFuture;

import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.view.request.RequestStatus;
import org.folio.inventory.domain.view.request.StoredRequestView;
import org.folio.inventory.storage.external.Clients;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.validation.ItemMarkAsWithdrawnValidators;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import io.vertx.core.json.JsonArray;

public class ItemService {
  private final ItemCollection itemCollection;
  private final CollectionResourceClient requestStorageClient;

  public ItemService(ItemCollection itemCollection, Clients clients) {
    this.itemCollection = itemCollection;
    this.requestStorageClient = clients.getRequestStorageClient();
  }

  public CompletableFuture<Item> processMarkItemWithdrawn(WebContext context) {
    final String itemId = context.getStringParameter("id", null);

    return itemCollection.findById(itemId)
      .thenCompose(ItemMarkAsWithdrawnValidators::itemIsFound)
      .thenCompose(ItemMarkAsWithdrawnValidators::itemHasAllowedStatusToMarkAsWithdrawn)
      .thenCompose(this::updateRequestStatusIfRequired)
      .thenApply(item -> item.changeStatus(ItemStatusName.WITHDRAWN))
      .thenCompose(itemCollection::update);
  }

  private CompletableFuture<Item> updateRequestStatusIfRequired(Item item) {
    final CqlQuery query = exactMatch("itemId", item.id)
      .and(exactMatch("status", RequestStatus.OPEN_AWAITING_PICKUP.getValue()));

    return requestStorageClient.getMany(query, 1, 0)
      .thenApply(response -> {
        final JsonArray requests = response.getJson().getJsonArray("requests");
        return requests != null && requests.size() > 0
          ? new StoredRequestView(requests.getJsonObject(0).getMap())
          : null;
      }).thenCompose(request -> {
        if (request != null && request.getHoldShelfExpirationDate() != null
          && request.getHoldShelfExpirationDate().isAfter(currentDateTime())) {

          request.changeStatus(RequestStatus.OPEN_NOT_YET_FILLED);
          return requestStorageClient.put(request.getId(), request.getMap())
            .thenApply(notUsed -> item);
        }

        return CompletableFuture.completedFuture(item);
      });
  }

  private DateTime currentDateTime() {
    return DateTime.now(DateTimeZone.UTC);
  }
}
