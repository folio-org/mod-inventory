package org.folio.inventory.services;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.storage.external.CqlQuery.exactMatch;
import static org.folio.inventory.storage.external.CqlQuery.match;
import static org.folio.inventory.storage.external.CqlQuery.notEqual;
import static org.folio.inventory.storage.external.Limit.one;
import static org.folio.inventory.storage.external.Offset.noOffset;
import static org.folio.inventory.support.JsonArrayHelper.toList;

import java.util.concurrent.CompletableFuture;

import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.view.request.RequestStatus;
import org.folio.inventory.domain.view.request.StoredRequestView;
import org.folio.inventory.storage.external.Clients;
import org.folio.inventory.storage.external.CollectionResourceRepository;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.validation.ItemMarkAsWithdrawnValidators;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemService {
  private static final Logger log = LoggerFactory.getLogger(ItemService.class);

  private final ItemCollection itemCollection;
  private final CollectionResourceRepository requestStorageRepository;

  public ItemService(ItemCollection itemCollection, Clients clients) {
    this.itemCollection = itemCollection;
    this.requestStorageRepository = clients.getRequestStorageRepository();
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
      .and(match("status", "Open"))
      .and(notEqual("status", RequestStatus.OPEN_NOT_YET_FILLED.getValue()));

    // Only one request in fulfilment is possible
    return requestStorageRepository.getMany(query, one(), noOffset())
      .thenApply(response -> response.getJson().getJsonArray("requests"))
      .thenApply(requests -> toList(requests, json -> new StoredRequestView(json.getMap())))
      .thenApply(requests -> requests.stream().findFirst())
      .thenCompose(requestOptional -> {
        if (!requestOptional.isPresent() || requestIsExpiredOnHoldShelf(requestOptional.get())) {
          log.debug("No request in fulfillment or it is expired");
          return completedFuture(item);
        }

        log.debug("Fount request in fulfillment {}", requestOptional.get().getId());
        return moveRequestIntoNotYetFilledStatus(requestOptional.get())
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
    return requestStorageRepository.put(request.getId(), request.getMap())
      .thenApply(notUsed -> request);
  }

  private DateTime currentDateTime() {
    return DateTime.now(DateTimeZone.UTC);
  }
}
