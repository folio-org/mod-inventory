package org.folio.inventory.storage.external.repository;

import static java.util.Arrays.asList;
import static org.folio.inventory.domain.view.request.RequestStatus.OPEN_AWAITING_DELIVERY;
import static org.folio.inventory.domain.view.request.RequestStatus.OPEN_AWAITING_PICKUP;
import static org.folio.inventory.domain.view.request.RequestStatus.OPEN_IN_TRANSIT;
import static org.folio.inventory.storage.external.CqlQuery.exactMatch;
import static org.folio.inventory.storage.external.CqlQuery.exactMatchAny;
import static org.folio.inventory.storage.external.Limit.one;
import static org.folio.inventory.storage.external.Offset.noOffset;
import static org.folio.inventory.support.JsonArrayHelper.toList;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.folio.inventory.domain.view.request.Request;
import org.folio.inventory.storage.external.Clients;
import org.folio.inventory.storage.external.CollectionResourceRepository;
import org.folio.inventory.storage.external.CqlQuery;

public class RequestRepository {
  private final CollectionResourceRepository requestStorageClient;

  public RequestRepository(Clients clients) {
    this.requestStorageClient = clients.getRequestStorageRepository();
  }

  public CompletableFuture<Optional<Request>> getRequestInFulfilmentForItem(String itemId) {
    final CqlQuery query = exactMatch("itemId", itemId)
      .and(exactMatchAny("status", getRequestInFulfillmentStatuses()));

    // Only one request in fulfilment is possible
    return requestStorageClient.getMany(query, one(), noOffset())
      .thenApply(response -> response.getJson().getJsonArray("requests"))
      .thenApply(requests -> toList(requests, Request::new))
      .thenApply(requests -> requests.stream().findFirst());
  }

  public CompletableFuture<Request> update(Request request) {
    return requestStorageClient.put(request.getId(), request.toJson())
      .thenApply(response -> request);
  }

  private List<String> getRequestInFulfillmentStatuses() {
    final List<String> statuses = asList(OPEN_AWAITING_PICKUP.getValue(),
      OPEN_AWAITING_DELIVERY.getValue(), OPEN_IN_TRANSIT.getValue());

    return Collections.unmodifiableList(statuses);
  }
}
