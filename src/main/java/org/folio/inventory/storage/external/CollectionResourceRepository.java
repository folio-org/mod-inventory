package org.folio.inventory.storage.external;

import java.util.concurrent.CompletableFuture;

import org.folio.inventory.exceptions.ExternalResourceFetchException;
import org.folio.inventory.support.http.client.Response;

public class CollectionResourceRepository {
  private final CollectionResourceClient resourceClient;

  public CollectionResourceRepository(CollectionResourceClient resourceClient) {
    this.resourceClient = resourceClient;
  }

  public CompletableFuture<Response> getMany(CqlQuery query, Limit limit, Offset offset) {
    final CompletableFuture<Response> future = new CompletableFuture<>();

    resourceClient.getMany(query.toString(), limit.getLimit(),
      offset.getOffset(), future::complete);

    return future.thenCompose(response -> handleResponse(response, 200));
  }

  public CompletableFuture<Response> post(Object resourceRepresentation) {
    CompletableFuture<Response> future = new CompletableFuture<>();
    resourceClient.post(resourceRepresentation, future::complete);

    return future.thenCompose(response -> handleResponse(response, 201));
  }

  public CompletableFuture<Response> put(String id, Object resourceRepresentation) {
    CompletableFuture<Response> future = new CompletableFuture<>();
    resourceClient.put(id, resourceRepresentation, future::complete);

    return future.thenCompose(response -> handleResponse(response, 204));
  }

  public CompletableFuture<Response> delete(String id) {
    CompletableFuture<Response> future = new CompletableFuture<>();

    resourceClient.delete(id, future::complete);

    return future.thenCompose(response -> handleResponse(response, 204));
  }

  private CompletableFuture<Response> handleResponse(Response response,
    int expectedStatusCode) {

    if (response.getStatusCode() != expectedStatusCode) {
      final CompletableFuture<Response> failed = new CompletableFuture<>();
      failed.completeExceptionally(new ExternalResourceFetchException(response));

      return failed;
    }

    return CompletableFuture.completedFuture(response);
  }
}
