package org.folio.inventory.dataimport.cache;

import java.net.URL;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.vertx.core.json.Json;
import io.vertx.ext.web.client.WebClient;
import lombok.SneakyThrows;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.CacheLoadingException;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;

/**
 * Cache for storing ProfileSnapshotWrapper entities by jobProfileSnapshotId
 */
public class ProfileSnapshotCache {

  private static final Logger LOGGER = LogManager.getLogger();

  private final AsyncCache<String, Optional<ProfileSnapshotWrapper>> cache;
  private final HttpClient httpClient;

  public ProfileSnapshotCache(Vertx vertx, HttpClient httpClient, long cacheExpirationTime) {
    this.httpClient = httpClient;
    cache = Caffeine.newBuilder()
      .expireAfterAccess(cacheExpirationTime, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync();
  }

  public Future<Optional<ProfileSnapshotWrapper>> get(String profileSnapshotId, Context context) {
    try {
      return Future.fromCompletionStage(cache.get(profileSnapshotId, (key, executor) -> loadJobProfileSnapshot(key, context)));
    } catch (Exception e) {
      LOGGER.warn("Error loading ProfileSnapshotWrapper by id: '{}'", profileSnapshotId, e);
      return Future.failedFuture(e);
    }
  }

  @SneakyThrows
  private CompletableFuture<Optional<ProfileSnapshotWrapper>> loadJobProfileSnapshot(String profileSnapshotId, Context context) {
    LOGGER.debug("Trying to load jobProfileSnapshot by id  '{}' for cache, okapi url: {}, tenantId: {}", profileSnapshotId, context.getOkapiLocation(), context.getTenantId());

    OkapiHttpClient client = new OkapiHttpClient(WebClient.wrap(httpClient), new URL(context.getOkapiLocation()), context.getTenantId(), context.getToken(), null, null, null);

    return client.get(context.getOkapiLocation() + "/data-import-profiles/jobProfileSnapshots/" + profileSnapshotId)
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
          LOGGER.info("JobProfileSnapshot was loaded by id '{}'", profileSnapshotId);
          return CompletableFuture.completedFuture(Optional.of(Json.decodeValue(httpResponse.getBody(), (ProfileSnapshotWrapper.class))));
        } else if (httpResponse.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("JobProfileSnapshot was not found by id '{}'", profileSnapshotId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format("Error loading jobProfileSnapshot by id: '%s', status code: %s, response message: %s",
            profileSnapshotId, httpResponse.getStatusCode(), httpResponse.getBody());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
      });
  }

}

