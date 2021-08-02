package org.folio.inventory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.consumers.QuickMarcKafkaHandler;
import org.folio.inventory.dataimport.handlers.QMEventTypes;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.kafka.cache.util.CacheUtil;
import org.folio.util.pubsub.PubSubClientUtils;

import static java.lang.String.format;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;
import static org.folio.kafka.KafkaTopicNameHelper.createSubscriptionDefinition;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

public class QuickMarcConsumerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger(QuickMarcConsumerVerticle.class);

  private static final long DELAY_TIME_BETWEEN_EVENTS_CLEANUP_VALUE_MILLIS = 3600000;
  private static final int EVENT_TIMEOUT_VALUE_HOURS = 3;

  private final int loadLimit = getLoadLimit();
  private final int maxDistributionNumber = getMaxDistributionNumber();
  private KafkaConsumerWrapper<String, String> consumer;

  @Override
  public void start(Promise<Void> startPromise) {
    JsonObject config = vertx.getOrCreateContext().config();
    KafkaConfig kafkaConfig = getKafkaConfig(config);

    HttpClient client = vertx.createHttpClient();
    Storage storage = Storage.basedUpon(vertx, config, client);

    KafkaInternalCache kafkaInternalCache = KafkaInternalCache.builder()
      .kafkaConfig(kafkaConfig)
      .build();
    kafkaInternalCache.initKafkaCache();

    var precedingSucceedingTitlesHelper = new PrecedingSucceedingTitlesHelper(WebClient.wrap(client));
    var handler = new QuickMarcKafkaHandler(vertx, storage, maxDistributionNumber, kafkaConfig, kafkaInternalCache, precedingSucceedingTitlesHelper);

    var kafkaConsumerFuture = createKafkaConsumer(kafkaConfig, QMEventTypes.QM_SRS_MARC_RECORD_UPDATED, handler);

    kafkaConsumerFuture
      .onFailure(startPromise::fail)
      .onSuccess(ar -> {
        consumer = ar;
        startPromise.complete();
      });

    CacheUtil.initCacheCleanupPeriodicTask(vertx, kafkaInternalCache, DELAY_TIME_BETWEEN_EVENTS_CLEANUP_VALUE_MILLIS,
      EVENT_TIMEOUT_VALUE_HOURS);
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
    LOGGER.info(format("kafkaConfig: %s", kafkaConfig));
    return kafkaConfig;
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumer.stop().onComplete(ar -> stopPromise.complete());
  }

  private Future<KafkaConsumerWrapper<String, String>> createKafkaConsumer(KafkaConfig kafkaConfig, QMEventTypes eventType,
                                                                           AsyncRecordHandler<String, String> recordHandler) {
    var subscriptionDefinition = createSubscriptionDefinition(kafkaConfig.getEnvId(),
      getDefaultNameSpace(), eventType.name());

    KafkaConsumerWrapper<String, String> consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    return consumerWrapper.start(recordHandler, PubSubClientUtils.constructModuleName())
      .map(consumerWrapper);
  }

  private int getLoadLimit() {
    return Integer.parseInt(System.getProperty("inventory.kafka.QuickMarcConsumer.loadLimit", "5"));
  }

  private int getMaxDistributionNumber() {
    return Integer.parseInt(System.getProperty("inventory.kafka.QuickMarcConsumerVerticle.maxDistributionNumber", "100"));
  }
}
