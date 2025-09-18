package org.folio.inventory.storage.external;

import static org.folio.util.PercentCodec.encode;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;

import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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

  /**
   * Run the query using some limit and offset.
   *
   * @param cqlQuery the query without percent (url) encoding
   */
  public void getMany(
    String cqlQuery,
    Integer pageLimit,
    Integer pageOffset,
    Consumer<Response> responseHandler) {

    getMany(cqlQuery, pageLimit, pageOffset)
    .thenAccept(responseHandler);
  }

  /**
   * Run the query using some limit and offset.
   *
   * @param cqlQuery the query without percent (url) encoding
   */
  public CompletableFuture<Response> getMany(
    String cqlQuery,
    Integer pageLimit,
    Integer pageOffset) {

    String url = collectionRoot + "?"
        + (isProvided(cqlQuery) ? ("query=" + encode(cqlQuery) + "&") : "")
        + "limit=" + pageLimit + "&offset=" + pageOffset;
    return client.get(url).toCompletableFuture();
  }

  /**
   * Run the query using some limit and offset.
   *
   * @param cqlQuery the query without percent (url) encoding
   */
  public void retrieveMany(
    String cqlQuery,
    Integer pageLimit,
    Integer pageOffset,
    Consumer<Response> responseHandler) {
    var body = new JsonObject()
      .put("query", cqlQuery)
      .put("limit", pageLimit)
      .put("offset", pageOffset);
    var url = collectionRoot + "/retrieve";
    client.post(url, body)
      .thenAccept(responseHandler);
  }

  /**
   * Runs the query while setting limit to maximum and offset to zero to get all records.
   *
   * @param cqlQuery the query without percent (url) encoding
   */
  public void getAll(String cqlQuery, Consumer<Response> responseHandler) {
    getMany(cqlQuery, Integer.MAX_VALUE, 0, responseHandler);
  }

  private boolean isProvided(String query) {
    return query != null && !query.trim().equals("");
  }

  private String recordUrl(String id) {
    return String.format(collectionRoot + "/%s", id);
  }
}
