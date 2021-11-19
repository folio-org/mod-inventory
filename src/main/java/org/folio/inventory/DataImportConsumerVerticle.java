package org.folio.inventory;

import static java.lang.String.format;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventTypes;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.cache.ProfileSnapshotCache;
import org.folio.inventory.dataimport.consumers.DataImportKafkaHandler;
import org.folio.inventory.dataimport.util.ConsumerWrapperUtil;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.kafka.cache.util.CacheUtil;
import org.folio.processing.events.EventManager;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonObject;

public class DataImportConsumerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger(DataImportConsumerVerticle.class);

  private static final long DELAY_TIME_BETWEEN_EVENTS_CLEANUP_VALUE_MILLIS = 3600000;
  private static final int EVENT_TIMEOUT_VALUE_HOURS = 3;
  private static final int DEFAULT_HTTP_TIMEOUT_IN_MILLISECONDS = 3000;

  private static final List<DataImportEventTypes> EVENT_TYPES = List.of(
    DI_SRS_MARC_BIB_RECORD_CREATED, DI_SRS_MARC_HOLDING_RECORD_CREATED,
    DI_SRS_MARC_BIB_RECORD_MODIFIED, DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING,
    DI_SRS_MARC_BIB_RECORD_MATCHED, DI_SRS_MARC_BIB_RECORD_NOT_MATCHED,
    DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING,
    DI_INVENTORY_INSTANCE_CREATED, DI_INVENTORY_INSTANCE_UPDATED,
    DI_INVENTORY_INSTANCE_MATCHED, DI_INVENTORY_INSTANCE_NOT_MATCHED,
    DI_INVENTORY_HOLDING_CREATED, DI_INVENTORY_HOLDING_UPDATED,
    DI_INVENTORY_HOLDING_MATCHED, DI_INVENTORY_HOLDING_NOT_MATCHED,
    DI_INVENTORY_ITEM_CREATED, DI_INVENTORY_ITEM_MATCHED,
    DI_INVENTORY_ITEM_NOT_MATCHED, DI_INVENTORY_ITEM_CREATED,
    DI_SRS_MARC_AUTHORITY_RECORD_CREATED);

  private final int loadLimit = getLoadLimit();
  private final int maxDistributionNumber = getMaxDistributionNumber();
  private List<KafkaConsumerWrapper<String, String>> consumerWrappers = new ArrayList<>();

  @Override
  public void start(Promise<Void> startPromise) {
    JsonObject config = vertx.getOrCreateContext().config();
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(config.getString(KAFKA_ENV))
      .kafkaHost(config.getString(KAFKA_HOST))
      .kafkaPort(config.getString(KAFKA_PORT))
      .okapiUrl(config.getString(OKAPI_URL))
      .replicationFactor(Integer.parseInt(config.getString(KAFKA_REPLICATION_FACTOR)))
      .maxRequestSize(Integer.parseInt(config.getString(KAFKA_MAX_REQUEST_SIZE)))
      .build();
    LOGGER.info(format("kafkaConfig: %s", kafkaConfig));
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, maxDistributionNumber);

    HttpClientOptions params = new HttpClientOptions().setConnectTimeout(DEFAULT_HTTP_TIMEOUT_IN_MILLISECONDS);
    HttpClient client = vertx.createHttpClient(params);
    Storage storage = Storage.basedUpon(vertx, config, client);

    KafkaInternalCache kafkaInternalCache = KafkaInternalCache.builder()
      .kafkaConfig(kafkaConfig)
      .build();
    kafkaInternalCache.initKafkaCache();

    String profileSnapshotExpirationTime = getCacheEnvVariable(config, "inventory.profile-snapshot-cache.expiration.time.seconds");
    String mappingMetadataExpirationTime = getCacheEnvVariable(config, "inventory.mapping-metadata-cache.expiration.time.seconds");

    ProfileSnapshotCache profileSnapshotCache = new ProfileSnapshotCache(vertx, client, Long.parseLong(profileSnapshotExpirationTime));
    MappingMetadataCache mappingMetadataCache = new MappingMetadataCache(vertx, client, Long.parseLong(mappingMetadataExpirationTime));


    DataImportKafkaHandler dataImportKafkaHandler = new DataImportKafkaHandler(vertx, storage, client, kafkaInternalCache, profileSnapshotCache, mappingMetadataCache);

    List<Future> futures = EVENT_TYPES.stream()
      .map(eventType -> createKafkaConsumerWrapper(kafkaConfig, eventType, dataImportKafkaHandler))
      .collect(Collectors.toList());

    CompositeFuture.all(futures)
      .onFailure(startPromise::fail)
      .onSuccess(ar -> {
        futures.forEach(future -> consumerWrappers.add((KafkaConsumerWrapper<String, String>) future.result()));
        startPromise.complete();
      });

    CacheUtil.initCacheCleanupPeriodicTask(vertx, kafkaInternalCache, DELAY_TIME_BETWEEN_EVENTS_CLEANUP_VALUE_MILLIS, EVENT_TIMEOUT_VALUE_HOURS);
  }


  @Override
  public void stop(Promise<Void> stopPromise) {
    List<Future> stopFutures = consumerWrappers.stream()
      .map(KafkaConsumerWrapper::stop)
      .collect(Collectors.toList());

    CompositeFuture.join(stopFutures).onComplete(ar -> stopPromise.complete());
  }

  private Future<KafkaConsumerWrapper<String, String>> createKafkaConsumerWrapper(KafkaConfig kafkaConfig, DataImportEventTypes eventType,
                                                                                  AsyncRecordHandler<String, String> recordHandler) {
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), eventType.value());

    KafkaConsumerWrapper<String, String> consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    return consumerWrapper.start(recordHandler, ConsumerWrapperUtil.constructModuleName())
      .map(consumerWrapper);
  }

  private int getLoadLimit() {
    return Integer.parseInt(System.getProperty("inventory.kafka.DataImportConsumer.loadLimit", "5"));
  }

  private int getMaxDistributionNumber() {
    return Integer.parseInt(System.getProperty("inventory.kafka.DataImportConsumerVerticle.maxDistributionNumber", "100"));
  }

  private String getCacheEnvVariable(JsonObject config, String variableName) {
    String cacheExpirationTime = config.getString(variableName);
    if (StringUtils.isBlank(cacheExpirationTime)) {
      cacheExpirationTime = "3600";
    }
    return cacheExpirationTime;
  }
}
