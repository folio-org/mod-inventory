package org.folio.inventory.consortium.services;

import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.entities.EntityLink;
import org.folio.inventory.consortium.exceptions.ConsortiumException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.consortium.util.ConsortiumUtil.createOkapiHttpClient;
public class EntitiesLinksServiceImpl implements EntitiesLinksService {
  private static final Logger LOGGER = LogManager.getLogger(EntitiesLinksServiceImpl.class);
  private static final String AUTHORITY_LINK = "/links/instances/%s";
  private final HttpClient httpClient;

  public EntitiesLinksServiceImpl(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  @Override
  public Future<List<EntityLink>> getInstanceAuthorityLinks(Context context, String instanceId) {
    CompletableFuture<List<EntityLink>> completableFuture = createOkapiHttpClient(context, httpClient)
      .thenCompose(client ->
        client.get(context.getOkapiLocation() + String.format(AUTHORITY_LINK, instanceId))
          .thenCompose(httpResponse -> {
            if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
              List<EntityLink> response = List.of(Json.decodeValue(Json.encode(httpResponse.getJson().getJsonArray("links")), EntityLink[].class));
              LOGGER.debug("shareInstance:: Successfully retrieved entities links for instance with id: {}", instanceId);
              return CompletableFuture.completedFuture(response);
            } else {
              String message = String.format("Error during retrieving entity links for instance with id: %s", instanceId);
              LOGGER.warn(String.format("shareInstance:: %s", message));
              return CompletableFuture.failedFuture(new ConsortiumException(message));
            }
          }));
    return Future.fromCompletionStage(completableFuture);
  }

  @Override
  public Future<List<EntityLink>> putInstanceAuthorityLinks(Context context, String instanceId, List<EntityLink> entityLinks) {
    JsonObject body = new JsonObject().put("links", JsonArray.of(entityLinks));
    CompletableFuture<List<EntityLink>> completableFuture = createOkapiHttpClient(context, httpClient)
      .thenCompose(client ->
        client.put(context.getOkapiLocation() + String.format(AUTHORITY_LINK, instanceId), body)
          .thenCompose(httpResponse -> {
            if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
              List<EntityLink> response = List.of(Json.decodeValue(Json.encode(httpResponse.getJson().getJsonArray("links")), EntityLink[].class));
              LOGGER.debug("shareInstance:: Successfully updated entities links for instance with id: {}", instanceId);
              return CompletableFuture.completedFuture(response);
            } else {
              String message = String.format("Error during updating entity links for instance with id: %s", instanceId);
              LOGGER.warn(String.format("shareInstance:: %s", message));
              return CompletableFuture.failedFuture(new ConsortiumException(message));
            }
          }));
    return Future.fromCompletionStage(completableFuture);
  }
}
