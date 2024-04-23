package org.folio.inventory.support;

import static java.lang.Integer.parseInt;
import static java.lang.Long.*;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.Objects.isNull;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;
import static org.folio.kafka.KafkaTopicNameHelper.createSubscriptionDefinition;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Verticle;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.okapi.common.GenericCompositeFuture;

public abstract class KafkaConsumerVerticle extends AbstractVerticle {
  private static final String LOAD_LIMIT_TEMPLATE = "inventory.kafka.%s.loadLimit";
  private static final String LOAD_LIMIT_DEFAULT = "5";
  private static final String MAX_DISTRIBUTION_NUMBER_TEMPLATE = "inventory.kafka.%s.maxDistributionNumber";
  private static final String MAX_DISTRIBUTION_NUMBER_DEFAULT = "100";
  private static final String CACHE_EXPIRATION_DEFAULT = "3600";
  private static final String METADATA_EXPIRATION_TIME = "inventory.mapping-metadata-cache.expiration.time.seconds";
  private final List<KafkaConsumerWrapper<String, String>> consumerWrappers = new ArrayList<>();
  private KafkaConfig kafkaConfig;
  private JsonObject config;
  private HttpClient httpClient;
  private Storage storage;
  private MappingMetadataCache mappingMetadataCache;

  @Override
  public void stop(Promise<Void> stopPromise) {
    var stopFutures = consumerWrappers.stream()
      .map(KafkaConsumerWrapper::stop)
      .toList();

    GenericCompositeFuture.join(stopFutures)
      .onComplete(ar -> stopPromise.complete());
  }

  protected abstract Logger getLogger();

  protected KafkaConsumerWrapper<String, String> createConsumer(String eventType) {
    var kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(getKafkaConfig())
      .loadLimit(getLoadLimit())
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(getSubscriptionDefinition(getKafkaConfig().getEnvId(), eventType))
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
    if (isNull(mappingMetadataCache)) {
      var mappingMetadataExpirationTime = getCacheEnvVariable(METADATA_EXPIRATION_TIME);
      mappingMetadataCache = new MappingMetadataCache(vertx, getHttpClient(), parseLong(mappingMetadataExpirationTime));
    }
    return mappingMetadataCache;
  }

  protected String getCacheEnvVariable(String variableName) {
    var cacheExpirationTime = getConfig().getString(variableName);
    if (isBlank(cacheExpirationTime)) {
      cacheExpirationTime = CACHE_EXPIRATION_DEFAULT;
    }
    return cacheExpirationTime;
  }

  protected int getMaxDistributionNumber() {
    return getConsumerProperty(MAX_DISTRIBUTION_NUMBER_TEMPLATE, MAX_DISTRIBUTION_NUMBER_DEFAULT);
  }

  private JsonObject getConfig() {
    if (isNull(config)) {
      config = vertx.getOrCreateContext().config();
    }
    return config;
  }

  private SubscriptionDefinition getSubscriptionDefinition(String envId, String eventType) {
    return createSubscriptionDefinition(envId, getDefaultNameSpace(), eventType);
  }

  private int getLoadLimit() {
    return getConsumerProperty(LOAD_LIMIT_TEMPLATE, LOAD_LIMIT_DEFAULT);
  }

  private int getConsumerProperty(String nameTemplate, String defaultValue) {
    var consumerClassName = getClass().getSimpleName();
    var cleanConsumerName = consumerClassName.substring(0, consumerClassName.indexOf(Verticle.class.getSimpleName()));
    return parseInt(getProperty(format(nameTemplate, cleanConsumerName), defaultValue));
  }

}
