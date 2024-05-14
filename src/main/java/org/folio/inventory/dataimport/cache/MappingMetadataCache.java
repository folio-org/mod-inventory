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
import org.folio.MappingMetadataDto;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.CacheLoadingException;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import org.folio.inventory.support.http.client.OkapiHttpClient;

/**
 * Cache for storing MappingMetadataDto entities by jobExecutionId
 */
public class MappingMetadataCache {

  private static final Logger LOGGER = LogManager.getLogger();
  private static MappingMetadataCache instance = null;
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

  public Future<Optional<MappingMetadataDto>> getByRecordType(String jobExecutionId, Context context, String recordType) {
    try {
      return Future.fromCompletionStage(cache.get(jobExecutionId, (key, executor) -> loadMappingMetadata(recordType, context)));
    } catch (Exception e) {
      LOGGER.warn("Error loading MappingMetadata by jobExecutionId: '{}'", jobExecutionId, e);
      return Future.failedFuture(e);
    }
  }

  @SneakyThrows
  private CompletableFuture<Optional<MappingMetadataDto>> loadJobProfileSnapshot(String jobExecutionId, Context context) {
    LOGGER.debug("Trying to load MappingMetadata by jobExecutionId  '{}' for cache, okapi url: {}, tenantId: {}", jobExecutionId, context.getOkapiLocation(), context.getTenantId());

    OkapiHttpClient client = new OkapiHttpClient(WebClient.wrap(httpClient), new URL(context.getOkapiLocation()), context.getTenantId(), context.getToken(), null, null, null);

    return client.get(context.getOkapiLocation() + "/mapping-metadata/" + jobExecutionId)
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
          LOGGER.info("MappingMetadata was loaded by jobExecutionId '{}'", jobExecutionId);
          return CompletableFuture.completedFuture(Optional.of(Json.decodeValue(httpResponse.getBody(), MappingMetadataDto.class)));
        } else if (httpResponse.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
         LOGGER.warn("MappingMetadata was not found by jobExecutionId '{}'", jobExecutionId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format("Error loading MappingMetadata by id: '%s', status code: %s, response message: %s",
            jobExecutionId, httpResponse.getStatusCode(), httpResponse.getBody());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
      });
  }

  @SneakyThrows
  private CompletableFuture<Optional<MappingMetadataDto>> loadMappingMetadata(String recordType, Context context) {
    LOGGER.debug("Trying to load MappingMetadata by recordType  '{}' for cache, okapi url: {}, tenantId: {}", recordType, context.getOkapiLocation(), context.getTenantId());

    OkapiHttpClient client = new OkapiHttpClient(WebClient.wrap(httpClient), new URL(context.getOkapiLocation()), context.getTenantId(), context.getToken(), null, null, null);

    return client.get(context.getOkapiLocation() + "/mapping-metadata/type/" + recordType)
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
          LOGGER.info("MappingMetadata was loaded by recordType '{}'", recordType);
          return CompletableFuture.completedFuture(Optional.of(Json.decodeValue(httpResponse.getBody(), MappingMetadataDto.class)));
        } else if (httpResponse.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("MappingMetadata was not found by recordType '{}'", recordType);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format("Error loading MappingMetadata by recordType: '%s', status code: %s, response message: %s",
            recordType, httpResponse.getStatusCode(), httpResponse.getBody());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
      });
  }

  public static synchronized MappingMetadataCache getInstance(Vertx vertx, HttpClient httpClient, long cacheExpirationTime) {
    if (instance == null) {
      instance = new MappingMetadataCache(vertx, httpClient, cacheExpirationTime);
    }
    return instance;
  }

}
