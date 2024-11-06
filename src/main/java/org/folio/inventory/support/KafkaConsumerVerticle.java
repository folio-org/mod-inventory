package org.folio.inventory.support;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.lang.System.getProperty;
import static java.util.Objects.isNull;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.cache.ProfileSnapshotCache;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.okapi.common.GenericCompositeFuture;

public abstract class KafkaConsumerVerticle extends AbstractVerticle {
  private static final String LOAD_LIMIT_TEMPLATE = "inventory.kafka.%s.loadLimit";
  private static final String LOAD_LIMIT_DEFAULT = "5";
  private static final String MAX_DISTRIBUTION_NUMBER_TEMPLATE = "inventory.kafka.%s.maxDistributionNumber";
  private static final String MAX_DISTRIBUTION_NUMBER_DEFAULT = "100";
  private static final String CACHE_EXPIRATION_DEFAULT = "3600";
  private static final String PROFILE_SNAPSHOT_CACHE_EXPIRATION_TIME = "inventory.profile-snapshot-cache.expiration.time.seconds";
  private final List<KafkaConsumerWrapper<String, String>> consumerWrappers = new ArrayList<>();
  private ProfileSnapshotCache profileSnapshotCache;
  private KafkaConfig kafkaConfig;
  private JsonObject config;
  private HttpClient httpClient;
  private Storage storage;

  @Override
  public void stop(Promise<Void> stopPromise) {
    var stopFutures = consumerWrappers.stream()
      .map(KafkaConsumerWrapper::stop)
      .toList();

    GenericCompositeFuture.join(stopFutures)
      .onComplete(ar -> stopPromise.complete());
  }

  protected abstract Logger getLogger();

  protected KafkaConsumerWrapper<String, String> createConsumer(String eventType, String loadLimitPropertyKey) {
    var loadLimit = getLoadLimit(loadLimitPropertyKey);
    return createConsumer(eventType, loadLimit, true);
  }

  protected KafkaConsumerWrapper<String, String> createConsumer(String eventType, int loadLimit, boolean namespacedTopic) {
    var kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(getKafkaConfig())
      .loadLimit(loadLimit)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(getSubscriptionDefinition(getKafkaConfig().getEnvId(), eventType, namespacedTopic))
      .build();
    consumerWrappers.add(kafkaConsumerWrapper);
    return kafkaConsumerWrapper;
  }

  protected KafkaConfig getKafkaConfig() {
    if (isNull(kafkaConfig)) {
      kafkaConfig = KafkaConfig.builder()
        .envId(getConfig().getString(KAFKA_ENV))
        .kafkaHost(getConfig().getString(KAFKA_HOST))
        .kafkaPort(getConfig().getString(KAFKA_PORT))
        .okapiUrl(getConfig().getString(OKAPI_URL))
        .replicationFactor(parseInt(getConfig().getString(KAFKA_REPLICATION_FACTOR)))
        .maxRequestSize(parseInt(getConfig().getString(KAFKA_MAX_REQUEST_SIZE)))
        .build();
      getLogger().info("kafkaConfig: {}", kafkaConfig);
    }
    return kafkaConfig;
  }

  protected HttpClient getHttpClient() {
    if (isNull(httpClient)) {
      httpClient = vertx.createHttpClient();
    }
    return httpClient;
  }

  protected Storage getStorage() {
    if (isNull(storage)) {
      storage = Storage.basedUpon(getConfig(), vertx.createHttpClient());
    }
    return storage;
  }

  protected MappingMetadataCache getMappingMetadataCache() {
    return MappingMetadataCache.getInstance(vertx, getHttpClient());
  }

  protected ProfileSnapshotCache getProfileSnapshotCache() {
    if (isNull(profileSnapshotCache)) {
      var profileSnapshotExpirationTime = getCacheEnvVariable(PROFILE_SNAPSHOT_CACHE_EXPIRATION_TIME);
      profileSnapshotCache = new ProfileSnapshotCache(vertx, getHttpClient(), Long.parseLong(profileSnapshotExpirationTime));
    }
    return profileSnapshotCache;
  }

  protected String getCacheEnvVariable(String variableName) {
    var cacheExpirationTime = getConfig().getString(variableName);
    if (isBlank(cacheExpirationTime)) {
      cacheExpirationTime = CACHE_EXPIRATION_DEFAULT;
    }
    return cacheExpirationTime;
  }

  protected int getMaxDistributionNumber(String property) {
    return getConsumerProperty(MAX_DISTRIBUTION_NUMBER_TEMPLATE, property, MAX_DISTRIBUTION_NUMBER_DEFAULT);
  }

  protected int getLoadLimit(String propertyKey, String defaultValue) {
    return getConsumerProperty(LOAD_LIMIT_TEMPLATE, propertyKey, defaultValue);
  }

  private JsonObject getConfig() {
    if (isNull(config)) {
      config = vertx.getOrCreateContext().config();
    }
    return config;
  }

  private SubscriptionDefinition getSubscriptionDefinition(String envId, String eventType, boolean namespacedTopic) {
    return namespacedTopic
      ? KafkaTopicNameHelper.createSubscriptionDefinition(envId, getDefaultNameSpace(), eventType)
      : createSubscriptionDefinition(envId, eventType);
  }

  private SubscriptionDefinition createSubscriptionDefinition(String env, String eventType) {
    return SubscriptionDefinition.builder()
      .eventType(eventType)
      .subscriptionPattern(formatSubscriptionPattern(env, eventType))
      .build();
  }

  private String formatSubscriptionPattern(String env, String eventType) {
    return join("\\.", env, "\\w{1,}", eventType);
  }

  private int getLoadLimit(String propertyKey) {
    return getConsumerProperty(LOAD_LIMIT_TEMPLATE, propertyKey, LOAD_LIMIT_DEFAULT);
  }

  private int getConsumerProperty(String nameTemplate, String propertyKey, String defaultValue) {
    return parseInt(getProperty(format(nameTemplate, propertyKey), defaultValue));
  }

}
