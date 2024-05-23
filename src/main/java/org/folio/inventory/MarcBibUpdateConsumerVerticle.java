package org.folio.inventory;

import static org.folio.inventory.dataimport.util.ConsumerWrapperUtil.constructModuleName;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.consumers.MarcBibUpdateKafkaHandler;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.SubscriptionDefinition;

public class MarcBibUpdateConsumerVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LogManager.getLogger(MarcBibUpdateConsumerVerticle.class);
  private static final GlobalLoadSensor GLOBAL_LOAD_SENSOR = new GlobalLoadSensor();
  private static final String SRS_MARC_BIB_TOPIC_NAME = "srs.marc-bib";
  private static final String METADATA_EXPIRATION_TIME = "inventory.mapping-metadata-cache.expiration.time.seconds";
  private final int loadLimit = getLoadLimit();
  private KafkaConsumerWrapper<String, String> marcBibUpdateConsumerWrapper;

  @Override
  public void start(Promise<Void> startPromise) {
    JsonObject config = vertx.getOrCreateContext().config();
    KafkaConfig kafkaConfig = getKafkaConfig(config);

    HttpClient client = vertx.createHttpClient();
    Storage storage = Storage.basedUpon(config, client);
    InstanceUpdateDelegate instanceUpdateDelegate = new InstanceUpdateDelegate(storage);

    MappingMetadataCache mappingMetadataCache = MappingMetadataCache.getInstance(vertx, client);

    MarcBibUpdateKafkaHandler marcBibUpdateKafkaHandler = new MarcBibUpdateKafkaHandler(vertx,
      getMaxDistributionNumber(), kafkaConfig, instanceUpdateDelegate, mappingMetadataCache);

    marcBibUpdateConsumerWrapper = createConsumer(kafkaConfig, SRS_MARC_BIB_TOPIC_NAME);
    marcBibUpdateConsumerWrapper.start(marcBibUpdateKafkaHandler, constructModuleName())
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  private KafkaConsumerWrapper<String, String> createConsumer(KafkaConfig kafkaConfig, String topicEventType) {
    SubscriptionDefinition subscriptionDefinition = SubscriptionDefinition.builder()
      .eventType(topicEventType)
      .subscriptionPattern(formatSubscriptionPattern(kafkaConfig.getEnvId(), topicEventType))
      .build();

    return KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(GLOBAL_LOAD_SENSOR)
      .subscriptionDefinition(subscriptionDefinition)
      .build();
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    marcBibUpdateConsumerWrapper.stop()
      .onComplete(ar -> stopPromise.complete());
  }

  private int getLoadLimit() {
    return Integer.parseInt(System.getProperty("inventory.kafka.MarcBibUpdateConsumer.loadLimit","5"));
  }

  private int getMaxDistributionNumber() {
    return Integer.parseInt(System.getProperty("inventory.kafka.MarcBibUpdateConsumer.maxDistributionNumber", "100"));
  }

  private KafkaConfig getKafkaConfig(JsonObject config) {
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(config.getString(KAFKA_ENV))
      .kafkaHost(config.getString(KAFKA_HOST))
      .kafkaPort(config.getString(KAFKA_PORT))
      .okapiUrl(config.getString(OKAPI_URL))
      .replicationFactor(Integer.parseInt(config.getString(KAFKA_REPLICATION_FACTOR)))
      .maxRequestSize(Integer.parseInt(config.getString(KAFKA_MAX_REQUEST_SIZE)))
      .build();
    LOGGER.info("kafkaConfig: {}", kafkaConfig);
    return kafkaConfig;
  }

  public static String formatSubscriptionPattern(String env, String eventType) {
    return String.join("\\.", env, "\\w{1,}", eventType);
  }
}
