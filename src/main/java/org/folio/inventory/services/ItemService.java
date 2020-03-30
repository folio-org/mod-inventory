package org.folio.inventory.services;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.storage.external.CqlQuery.exactMatch;
import static org.folio.inventory.support.CompletableFutures.failedFuture;
import static org.folio.inventory.support.JsonArrayHelper.toList;

import java.util.concurrent.CompletableFuture;

import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.view.request.RequestStatus;
import org.folio.inventory.domain.view.request.StoredRequestView;
import org.folio.inventory.exceptions.ExternalResourceFetchException;
import org.folio.inventory.storage.external.Clients;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.validation.ItemMarkAsWithdrawnValidators;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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

    return requestStorageClient.getSingle(query)
      .thenCompose(response -> response.getStatusCode() == 200
        ? completedFuture(response)
        : failedFuture(new ExternalResourceFetchException(response)))
      .thenApply(response -> response.getJson().getJsonArray("requests"))
      .thenApply(requests -> toList(requests, json -> new StoredRequestView(json.getMap())))
      .thenCompose(requests -> {
        if (requests.size() == 0 || requestIsExpiredOnHoldShelf(requests.get(0))) {
          return completedFuture(item);
        }

        return moveRequestIntoNotYetFilledStatus(requests.get(0))
          .thenApply(notUsed -> item);
      });
  }

  private boolean requestIsExpiredOnHoldShelf(StoredRequestView request) {
    return request.getHoldShelfExpirationDate() != null
      && currentDateTime().isAfter(request.getHoldShelfExpirationDate());
  }

  private CompletableFuture<StoredRequestView> moveRequestIntoNotYetFilledStatus(
    StoredRequestView request) {

    request.setStatus(RequestStatus.OPEN_NOT_YET_FILLED);
    return requestStorageClient.put(request.getId(), request.getMap())
      .thenApply(notUsed -> request);
  }

  private DateTime currentDateTime() {
    return DateTime.now(DateTimeZone.UTC);
  }
}
