package org.folio.inventory.dataimport.cache;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.CacheLoadingException;
import org.folio.rest.client.MappingMetadataClient;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.rest.util.OkapiConnectionParams;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;

/**
 * Cache for storing MappingMetadataDto entities by jobExecutionId
 */
public class MappingMetadataCache {

  private static final Logger LOGGER = LogManager.getLogger();

  private final AsyncCache<String, Optional<MappingMetadataDto>> cache;
  private final HttpClient httpClient;

  public MappingMetadataCache(Vertx vertx, HttpClient httpClient, long cacheExpirationTime) {
    this.httpClient = httpClient;
    cache = Caffeine.newBuilder()
      .expireAfterAccess(cacheExpirationTime, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync();
  }

  public Future<Optional<MappingMetadataDto>> get(String jobExecutionId, Context context) {
    try {
      return Future.fromCompletionStage(cache.get(jobExecutionId, (key, executor) -> loadJobProfileSnapshot(key, context)));
    } catch (Exception e) {
      LOGGER.warn("Error loading MappingMetadata by jobExecutionId: '{}'", jobExecutionId, e);
      return Future.failedFuture(e);
    }
  }

  private CompletableFuture<Optional<MappingMetadataDto>> loadJobProfileSnapshot(String jobExecutionId, Context context) {
    LOGGER.debug("Trying to load MappingMetadata by jobExecutionId  '{}' for cache, okapi url: {}, tenantId: {}", jobExecutionId, context.getOkapiLocation(), context.getTenantId());

    MappingMetadataClient client = new MappingMetadataClient(context.getOkapiLocation(), context.getTenantId(), context.getToken(), httpClient);

    return client.getMappingMetadataByJobExecutionId(jobExecutionId)
      .toCompletionStage()
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.statusCode() == HttpStatus.SC_OK) {
          LOGGER.info("MappingMetadata was loaded by jobExecutionId '{}'", jobExecutionId);
          return CompletableFuture.completedFuture(Optional.of(httpResponse.bodyAsJson(MappingMetadataDto.class)));
        } else if (httpResponse.statusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("MappingMetadata was not found by jobExecutionId '{}'", jobExecutionId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format("Error loading MappingMetadata by id: '%s', status code: %s, response message: %s",
            jobExecutionId, httpResponse.statusCode(), httpResponse.bodyAsString());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
      });
  }

}
