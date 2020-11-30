package org.folio.inventory.storage.external;

import static org.apache.commons.collections4.ListUtils.partition;
import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.util.StringUtil.urlEncode;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.folio.inventory.exceptions.ExternalResourceFetchException;
import org.folio.inventory.support.CompletableFutures;
import org.folio.inventory.support.http.client.Response;

import io.vertx.core.json.JsonObject;

public class MultipleRecordsFetchClient {
  private static final int DEFAULT_PARTITION_SIZE = 30;

  private final CollectionResourceClient resourceClient;
  private final int partitionSize;
  private final String collectionPropertyName;
  private final int expectedStatus;

  private MultipleRecordsFetchClient(Builder builder) {
    this.resourceClient = builder.collectionResourceClient;
    this.partitionSize = builder.partitionSize;
    this.collectionPropertyName = builder.collectionPropertyName;
    this.expectedStatus = builder.expectedStatus;
  }

  public <T> CompletableFuture<List<JsonObject>> find(
    List<T> elements, Function<List<T>, CqlQuery> toQueryConverter) {

    final List<CompletableFuture<Response>> allFutures = partition(elements, partitionSize).stream()
      .map(toQueryConverter)
      .map(this::getAllMatched)
      .collect(Collectors.toList());

    return CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]))
      .thenApply(notUsed -> allFutures.stream()
        .map(CompletableFuture::join)
        .map(Response::getJson)
        .flatMap(json -> toList(json.getJsonArray(collectionPropertyName)).stream())
        .collect(Collectors.toList()));
  }

  private CompletableFuture<Response> getAllMatched(CqlQuery query) {
    final CompletableFuture<Response> future = new CompletableFuture<>();

    resourceClient.getMany(urlEncode(query.toString()), Integer.MAX_VALUE, 0,
      future::complete);

    return future.thenCompose(response -> {
      if (response.getStatusCode() != expectedStatus) {
        return CompletableFutures.failedFuture(new ExternalResourceFetchException(response));
      }

      return CompletableFuture.completedFuture(response);
    });
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private CollectionResourceClient collectionResourceClient;
    private int expectedStatus = 200;
    private String collectionPropertyName;
    private final int partitionSize = DEFAULT_PARTITION_SIZE;

    public Builder withCollectionResourceClient(CollectionResourceClient client) {
      this.collectionResourceClient = client;
      return this;
    }

    public Builder withCollectionPropertyName(String collectionName) {
      this.collectionPropertyName = collectionName;
      return this;
    }

    public Builder withExpectedStatus(int expectedStatus) {
      this.expectedStatus = expectedStatus;
      return this;
    }

    public MultipleRecordsFetchClient build() {
      if (collectionResourceClient == null || collectionPropertyName == null) {
        throw new IllegalStateException("Resource client and collection property name are required");
      }

      return new MultipleRecordsFetchClient(this);
    }
  }
}
