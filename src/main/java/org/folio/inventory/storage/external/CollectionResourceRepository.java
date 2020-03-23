package org.folio.inventory.storage.external;

import org.folio.inventory.storage.external.exceptions.ExternalResourceFetchException;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;

import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class CollectionResourceRepository extends CollectionResourceClient{

  public CollectionResourceRepository(OkapiHttpClient client, URL collectionRoot) {
    super(client, collectionRoot);
  }

  public CompletableFuture<Response> post(Object resourceRepresentation) {
    CompletableFuture<Response> future = new CompletableFuture<>();
    post(resourceRepresentation, future::complete);

    return future.thenCompose(response ->  handleResponse(response, 201));
  }

  public CompletableFuture<Response> put(String id, Object resourceRepresentation) {
    CompletableFuture<Response> future = new CompletableFuture<>();
    put(id, resourceRepresentation, future::complete);

    return future.thenCompose(response ->  handleResponse(response, 204));
  }

  public CompletableFuture<Response> delete(String id) {
    CompletableFuture<Response> future = new CompletableFuture<>();

    delete(id, future::complete);

    return future.thenCompose(response -> handleResponse(response, 204));
  }

  private CompletionStage<Response> handleResponse(Response response,
    int expectedStatusCode) {

    if (response.getStatusCode() != expectedStatusCode) {
      final CompletableFuture<Response> failed = new CompletableFuture<>();
      failed.completeExceptionally(new ExternalResourceFetchException(response));

      return failed;
    }

    return CompletableFuture.completedFuture(response);
  }
}
