package org.folio.inventory.dataimport.cache;

import static java.lang.String.format;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.MappingMetadataDto;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.CacheLoadingException;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.SynchronousHttpClient;

/**
 * Cache for storing MappingMetadataDto entities by jobExecutionId
 */
public class MappingMetadataCache {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String METADATA_EXPIRATION_TIME = "inventory.mapping-metadata-cache.expiration.time.seconds";
  private static final String MAPPING_PARAM_LOAD_ERROR_LOG_MSG_TEMPLATE =
    "Error loading MappingMetadata by jobExecutionId: '{}'";
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

  public static MappingMetadataCache getInstance(Vertx vertx, HttpClient httpClient) {
    return getInstance(vertx, httpClient, false);
  }

  /**
   * Used for testing
   */
  public static synchronized MappingMetadataCache getInstance(Vertx vertx, HttpClient httpClient, boolean returnNew) {
    if (instance == null || returnNew) {
      instance = new MappingMetadataCache(vertx, httpClient,
        Long.parseLong(getCacheEnvVariable(vertx.getOrCreateContext().config(), METADATA_EXPIRATION_TIME)));
    }
    return instance;
  }

  public Future<Optional<MappingMetadataDto>> get(String jobExecutionId, Context context) {
    try {
      return Future.fromCompletionStage(
        cache.get(jobExecutionId, (key, executor) -> loadJobProfileSnapshot(key, context)));
    } catch (Exception e) {
      LOGGER.warn(MAPPING_PARAM_LOAD_ERROR_LOG_MSG_TEMPLATE, jobExecutionId, e);
      return Future.failedFuture(e);
    }
  }

  public Future<Optional<MappingMetadataDto>> getByRecordType(String jobExecutionId, Context context,
                                                              String recordType) {
    try {
      return Future.fromCompletionStage(
        cache.get(jobExecutionId, (key, executor) -> loadMappingMetadata(recordType, context)));
    } catch (Exception e) {
      LOGGER.warn(MAPPING_PARAM_LOAD_ERROR_LOG_MSG_TEMPLATE, jobExecutionId, e);
      return Future.failedFuture(e);
    }
  }

  public Optional<MappingMetadataDto> getByRecordTypeBlocking(String jobExecutionId, Context context,
                                                              String recordType) {
    try {
      var mapping = cache.synchronous().asMap().getOrDefault(jobExecutionId, Optional.empty());
      if (mapping.isPresent()) {
        return mapping;
      }
      mapping = getMappingMetadata(recordType, context);
      cache.put(jobExecutionId, CompletableFuture.completedFuture(mapping));
      return mapping;
    } catch (Exception e) {
      LOGGER.warn(MAPPING_PARAM_LOAD_ERROR_LOG_MSG_TEMPLATE, jobExecutionId, e);
      throw e;
    }
  }

  @SneakyThrows
  private CompletableFuture<Optional<MappingMetadataDto>> loadJobProfileSnapshot(String jobExecutionId,
                                                                                 Context context) {
    LOGGER.debug("Trying to load MappingMetadata by jobExecutionId  '{}' for cache, okapi url: {}, tenantId: {}",
      jobExecutionId, context.getOkapiLocation(), context.getTenantId());

    OkapiHttpClient client = new OkapiHttpClient(WebClient.wrap(httpClient),
      new URI(context.getOkapiLocation()).toURL(), context.getTenantId(), context.getToken(), null, null, null);

    return client.get(context.getOkapiLocation() + "/mapping-metadata/" + jobExecutionId)
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.statusCode() == HttpStatus.SC_OK) {
          LOGGER.info("MappingMetadata was loaded by jobExecutionId '{}'", jobExecutionId);
          return CompletableFuture.completedFuture(
            Optional.of(Json.decodeValue(httpResponse.body(), MappingMetadataDto.class)));
        } else if (httpResponse.statusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("MappingMetadata was not found by jobExecutionId '{}'", jobExecutionId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = format("Error loading MappingMetadata by id: '%s', status code: %s, response message: %s",
            jobExecutionId, httpResponse.statusCode(), httpResponse.body());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
      });
  }

  @SneakyThrows
  private CompletableFuture<Optional<MappingMetadataDto>> loadMappingMetadata(String recordType, Context context) {
    LOGGER.debug("Trying to load MappingMetadata by recordType  '{}' for cache, okapi url: {}, tenantId: {}",
      recordType, context.getOkapiLocation(), context.getTenantId());

    OkapiHttpClient client = new OkapiHttpClient(WebClient.wrap(httpClient),
      new URI(context.getOkapiLocation()).toURL(), context.getTenantId(), context.getToken(), null, null, null);

    return client.get(context.getOkapiLocation() + "/mapping-metadata/type/" + recordType)
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.statusCode() == HttpStatus.SC_OK) {
          LOGGER.info("MappingMetadata was loaded by recordType '{}'", recordType);
          return CompletableFuture.completedFuture(
            Optional.of(Json.decodeValue(httpResponse.body(), MappingMetadataDto.class)));
        } else if (httpResponse.statusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("MappingMetadata was not found by recordType '{}'", recordType);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message =
            format("Error loading MappingMetadata by recordType: '%s', status code: %s, response message: %s",
              recordType, httpResponse.statusCode(), httpResponse.body());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
      });
  }

  @SneakyThrows
  private Optional<MappingMetadataDto> getMappingMetadata(String recordType, Context context) {
    LOGGER.debug("Trying to get MappingMetadata by recordType  '{}' for cache, okapi url: {}, tenantId: {}", recordType,
      context.getOkapiLocation(), context.getTenantId());

    var httpSyncClient = new SynchronousHttpClient(context);
    var response = httpSyncClient.get(context.getOkapiLocation() + "/mapping-metadata/type/" + recordType);

    if (response.statusCode() == HttpStatus.SC_OK) {
      LOGGER.info("MappingMetadata was fetched by recordType '{}'", recordType);
      return Optional.of(Json.decodeValue(response.body(), MappingMetadataDto.class));
    } else if (response.statusCode() == HttpStatus.SC_NOT_FOUND) {
      LOGGER.warn("MappingMetadata was not found by recordType '{}'", recordType);
      return Optional.empty();
    } else {
      String message =
        format("Error loading MappingMetadata by recordType: '%s', status code: %s, response message: %s",
          recordType, response.statusCode(), response.body());
      LOGGER.warn(message);
      throw new CacheLoadingException(message);
    }
  }

  private static String getCacheEnvVariable(JsonObject config, String variableName) {
    String cacheExpirationTime = config.getString(variableName);
    if (StringUtils.isBlank(cacheExpirationTime)) {
      cacheExpirationTime = "3600";
    }
    return cacheExpirationTime;
  }
}
