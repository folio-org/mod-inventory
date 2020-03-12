package org.folio.inventory.storage.external;

import static org.apache.commons.collections4.ListUtils.partition;
import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.util.StringUtil.urlEncode;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.folio.inventory.storage.external.exceptions.ExternalResourceFetchException;
import org.folio.inventory.support.http.client.Response;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public class MultipleRecordsFetchClient {
  private static final int DEFAULT_PARTITION_SIZE = 30;

  private final CollectionResourceClient resourceClient;
  private final int partitionSize;
  private final String collectionPropertyName;
  private final int expectStatus;

  private MultipleRecordsFetchClient(Builder builder) {
    this.resourceClient = builder.collectionResourceClient;
    this.partitionSize = builder.partitionSize;
    this.collectionPropertyName = builder.collectionPropertyName;
    this.expectStatus = builder.expectStatus;
  }

  public <T> Future<List<JsonObject>> find(
    List<T> elements, Function<List<T>, CqlQuery> toQueryConverter) {

    final List<Future<Response>> allFutures = partition(elements, partitionSize).stream()
      .map(toQueryConverter)
      .map(this::getAllMatched)
      .collect(Collectors.toList());

    return CompositeFuture.all(Collections.unmodifiableList(allFutures))
      .map(CompositeFuture::<Response>list)
      .map(list -> list.stream()
        .map(Response::getJson)
        .flatMap(json -> toList(json.getJsonArray(collectionPropertyName)).stream())
        .collect(Collectors.toList()));
  }

  private Future<Response> getAllMatched(CqlQuery query) {
    final Future<Response> future = Future.future();

    resourceClient.getMany(urlEncode(query.toString()), Integer.MAX_VALUE, 0,
      future::complete);

    return future.compose(response -> {
      if (response.getStatusCode() != expectStatus) {
        return Future.failedFuture(new ExternalResourceFetchException(response));
      }

      return Future.succeededFuture(response);
    });
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private CollectionResourceClient collectionResourceClient;
    private int expectStatus = 200;
    private String collectionPropertyName;
    private int partitionSize = DEFAULT_PARTITION_SIZE;

    public Builder withCollectionResourceClient(CollectionResourceClient client) {
      this.collectionResourceClient = client;
      return this;
    }

    public Builder withCollectionPropertyName(String collectionName) {
      this.collectionPropertyName = collectionName;
      return this;
    }

    public Builder withExpectedStatus(int expectedStatus) {
      this.expectStatus = expectedStatus;
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
