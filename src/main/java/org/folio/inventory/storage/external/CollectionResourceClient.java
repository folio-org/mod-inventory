package org.folio.inventory.storage.external;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClientResponse;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;

import java.net.URL;
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

    client.post(collectionRoot,
      resourceRepresentation,
      responseConversationHandler(responseHandler));
  }

  public void put(String id, Object resourceRepresentation,
                  Consumer<Response> responseHandler) {

    client.put(String.format(collectionRoot + "/%s", id),
      resourceRepresentation,
      responseConversationHandler(responseHandler));
  }

  public void get(String id, Consumer<Response> responseHandler) {
    client.get(String.format(collectionRoot + "/%s", id),
      responseConversationHandler(responseHandler));
  }

  public void delete(String id, Consumer<Response> responseHandler) {
    client.delete(String.format(collectionRoot + "/%s", id),
      responseConversationHandler(responseHandler));
  }

  public void delete(Consumer<Response> responseHandler) {
    client.delete(collectionRoot,
      responseConversationHandler(responseHandler));
  }

  public void getMany(String query, Consumer<Response> responseHandler) {

    String url = isProvided(query)
      ? String.format("%s?%s", collectionRoot, query)
      : collectionRoot.toString();

    client.get(url,
      responseConversationHandler(responseHandler));
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

    client.get(url, responseConversationHandler(responseHandler));
  }

  private boolean isProvided(String query) {
    return query != null && query.trim() != "";
  }

  private Handler<HttpClientResponse> responseConversationHandler(
    Consumer<Response> responseHandler) {

    return response ->
      response.bodyHandler(buffer ->
        responseHandler.accept(Response.from(response, buffer)));
  }
}
