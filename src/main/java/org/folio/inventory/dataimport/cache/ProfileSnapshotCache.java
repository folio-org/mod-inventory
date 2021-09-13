package org.folio.inventory.dataimport.cache;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.exceptions.CacheLoadingException;
import org.folio.rest.client.DataImportProfilesClient;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.util.OkapiConnectionParams;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;

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

  public Future<Optional<ProfileSnapshotWrapper>> get(String profileSnapshotId, Map<String, String> context) {
    try {
      return Future.fromCompletionStage(cache.get(profileSnapshotId, (key, executor) -> loadJobProfileSnapshot(key, context)));
    } catch (Exception e) {
      LOGGER.warn("Error loading ProfileSnapshotWrapper by id: '{}'", profileSnapshotId, e);
      return Future.failedFuture(e);
    }
  }

  private CompletableFuture<Optional<ProfileSnapshotWrapper>> loadJobProfileSnapshot(String profileSnapshotId, Map<String, String> context) {
    LOGGER.debug("Trying to load jobProfileSnapshot by id  '{}' for cache, okapi url: {}, tenantId: {}", profileSnapshotId, context.get(OkapiConnectionParams.OKAPI_URL_HEADER), context.get(OkapiConnectionParams.OKAPI_TENANT_HEADER));

    DataImportProfilesClient client = new DataImportProfilesClient(context.get(OkapiConnectionParams.OKAPI_URL_HEADER), context.get(OkapiConnectionParams.OKAPI_TENANT_HEADER), context.get(OkapiConnectionParams.OKAPI_TOKEN_HEADER), httpClient);

    return client.getDataImportProfilesJobProfileSnapshotsById(profileSnapshotId)
      .toCompletionStage()
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.statusCode() == HttpStatus.SC_OK) {
          LOGGER.info("JobProfileSnapshot was loaded by id '{}'", profileSnapshotId);
          return CompletableFuture.completedFuture(Optional.of(httpResponse.bodyAsJson(ProfileSnapshotWrapper.class)));
        } else if (httpResponse.statusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("JobProfileSnapshot was not found by id '{}'", profileSnapshotId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format("Error loading jobProfileSnapshot by id: '%s', status code: %s, response message: %s",
            profileSnapshotId, httpResponse.statusCode(), httpResponse.bodyAsString());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
      });
  }

}

