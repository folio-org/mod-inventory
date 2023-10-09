package org.folio.inventory.services;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.exceptions.ConsortiumException;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.folio.inventory.consortium.util.ConsortiumUtil.DEFAULT_EXPIRATION_TIME_SECONDS;
import static org.folio.inventory.consortium.util.ConsortiumUtil.EXPIRATION_TIME_PARAM;
import static org.folio.inventory.consortium.util.ConsortiumUtil.createOkapiHttpClient;

public class EntitiesLinksServiceImpl implements EntitiesLinksService {
  private static final Logger LOGGER = LogManager.getLogger(EntitiesLinksServiceImpl.class);
  private static final String AUTHORITY_LINK_PATH = "/links/instances/%s";
  private static final String LINKING_RULES_PATH = "/linking-rules/instance-authority";
  public static final String LINKS = "links";
  private final HttpClient httpClient;
  private final AsyncCache<String, List<LinkingRuleDto>> linkingRulesCache;

  public EntitiesLinksServiceImpl(Vertx vertx, HttpClient httpClient) {
    int expirationTime = Integer.parseInt(System.getProperty(EXPIRATION_TIME_PARAM, DEFAULT_EXPIRATION_TIME_SECONDS));
    this.httpClient = httpClient;
    this.linkingRulesCache = Caffeine.newBuilder()
      .expireAfterWrite(expirationTime, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync();
  }

  @Override
  public Future<List<Link>> getInstanceAuthorityLinks(Context context, String instanceId) {
    CompletableFuture<List<Link>> completableFuture = createOkapiHttpClient(context, httpClient)
      .thenCompose(client ->
        client.get(context.getOkapiLocation() + String.format(AUTHORITY_LINK_PATH, instanceId))
          .thenCompose(httpResponse -> {
            if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
              List<Link> response = List.of(Json.decodeValue(Json.encode(httpResponse.getJson().getJsonArray(LINKS)), Link[].class));
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
  public Future<List<Link>> putInstanceAuthorityLinks(Context context, String instanceId, List<Link> entityLinks) {
    JsonObject body = new JsonObject().put(LINKS, new JsonArray(entityLinks));
    CompletableFuture<List<Link>> completableFuture = createOkapiHttpClient(context, httpClient)
      .thenCompose(client ->
        client.put(context.getOkapiLocation() + String.format(AUTHORITY_LINK_PATH, instanceId), body)
          .thenCompose(httpResponse -> {
            if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
              List<Link> response = List.of(Json.decodeValue(Json.encode(httpResponse.getJson().getJsonArray(LINKS)), Link[].class));
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

  @Override
  public Future<List<LinkingRuleDto>> getLinkingRules(Context context) {
    try {
      return Future.fromCompletionStage(linkingRulesCache.get(context.getTenantId(), (key, executor) -> loadLinkingRules(context)));
    } catch (Exception e) {
      LOGGER.warn("getLinkingRules:: Error loading linking rules data, tenantId: '{}'", context.getTenantId(), e);
      return Future.failedFuture(e);
    }
  }

  private CompletableFuture<List<LinkingRuleDto>> loadLinkingRules(Context context) {
    return createOkapiHttpClient(context, httpClient).thenCompose(client -> client.get(context.getOkapiLocation() + LINKING_RULES_PATH).thenCompose(httpResponse -> {
      if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
        List<LinkingRuleDto> linkingRules = List.of(Json.decodeValue(httpResponse.getBody(), LinkingRuleDto[].class));
        LOGGER.debug("loadLinkingRules:: Successfully loaded linking rules for tenant with id: {}", context.getTenantId());
        return CompletableFuture.completedFuture(linkingRules);
      } else {
        String message = String.format("Error during loading linking rules for tenant with id: %s", context.getTenantId());
        LOGGER.warn(String.format("loadLinkingRules:: %s", message));
        return CompletableFuture.failedFuture(new ConsortiumException(message));
      }
    }));
  }
}
