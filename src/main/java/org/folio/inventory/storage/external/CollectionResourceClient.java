package org.folio.inventory.storage.external;

import java.net.URL;
import java.util.function.Consumer;

import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;

public class CollectionResourceClient {

  private final OkapiHttpClient client;
  private final URL collectionRoot;

  public CollectionResourceClient(OkapiHttpClient client,
                                  URL collectionRoot) {

    this.client = client;
    this.collectionRoot = collectionRoot;
  }

  public void post(Object resourceRepresentation,
                   Consumer<Response> responseHandler) {

    client.post(collectionRoot, JsonObject.mapFrom(resourceRepresentation))
      .thenAccept(responseHandler);
  }

  public void put(String id, Object resourceRepresentation,
                  Consumer<Response> responseHandler) {

    client.put(recordUrl(id), JsonObject.mapFrom(resourceRepresentation))
      .thenAccept(responseHandler);
  }

  public void get(String id, Consumer<Response> responseHandler) {
    client.get(recordUrl(id))
      .thenAccept(responseHandler);
  }

  public void delete(String id, Consumer<Response> responseHandler) {
    client.delete(recordUrl(id))
      .thenAccept(responseHandler);
  }

  public void delete(Consumer<Response> responseHandler) {
    client.delete(collectionRoot)
      .thenAccept(responseHandler);
  }

  public void getMany(String query, Consumer<Response> responseHandler) {
    String url = isProvided(query)
      ? String.format("%s?%s", collectionRoot, query)
      : collectionRoot.toString();

    client.get(url)
      .thenAccept(responseHandler);
  }

  public void getMany(
    String cqlQuery,
    Integer pageLimit,
    Integer pageOffset,
    Consumer<Response> responseHandler) {

    //TODO: Replace with query string creator that checks each parameter
    String url = isProvided(cqlQuery)
      ? String.format("%s?query=%s&limit=%s&offset=%s", collectionRoot, cqlQuery,
      pageLimit, pageOffset)
      : collectionRoot.toString();

    client.get(url)
      .thenAccept(responseHandler);
  }

  private boolean isProvided(String query) {
    return query != null && !query.trim().equals("");
  }

  private String recordUrl(String id) {
    return String.format(collectionRoot + "/%s", id);
  }
}
