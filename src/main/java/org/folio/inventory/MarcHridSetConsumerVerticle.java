package org.folio.inventory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventTypes;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.consumers.MarcBibInstanceHridSetKafkaHandler;
import org.folio.inventory.dataimport.consumers.MarcHoldingsRecordHridSetKafkaHandler;
import org.folio.inventory.dataimport.handlers.actions.HoldingsUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.services.HoldingsCollectionService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;

import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_INSTANCE_HRID_SET;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET;
import static org.folio.inventory.dataimport.util.ConsumerWrapperUtil.constructModuleName;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;

public class MarcHridSetConsumerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger(MarcHridSetConsumerVerticle.class);
  private static final GlobalLoadSensor GLOBAL_LOAD_SENSOR = new GlobalLoadSensor();

  private final int loadLimit = getLoadLimit();
  private KafkaConsumerWrapper<String, String> marcBibConsumerWrapper;
  private KafkaConsumerWrapper<String, String> marcHoldingsConsumerWrapper;

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
    LOGGER.info("kafkaConfig: {}", kafkaConfig);

    marcBibConsumerWrapper = createConsumerByEvent(kafkaConfig, DI_SRS_MARC_BIB_INSTANCE_HRID_SET);
    marcHoldingsConsumerWrapper = createConsumerByEvent(kafkaConfig, DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET);

    HttpClient client = vertx.createHttpClient();
    Storage storage = Storage.basedUpon(config, client);
    HoldingsCollectionService holdingsCollectionService = new HoldingsCollectionService();
    InstanceUpdateDelegate instanceUpdateDelegate = new InstanceUpdateDelegate(storage);
    HoldingsUpdateDelegate holdingsRecordUpdateDelegate = new HoldingsUpdateDelegate(storage, holdingsCollectionService);

    MappingMetadataCache mappingMetadataCache = MappingMetadataCache.getInstance(vertx, client);

    MarcBibInstanceHridSetKafkaHandler marcBibInstanceHridSetKafkaHandler = new MarcBibInstanceHridSetKafkaHandler(instanceUpdateDelegate, mappingMetadataCache);
    MarcHoldingsRecordHridSetKafkaHandler marcHoldingsRecordHridSetKafkaHandler = new MarcHoldingsRecordHridSetKafkaHandler(holdingsRecordUpdateDelegate, mappingMetadataCache);

    CompositeFuture.all(
        marcBibConsumerWrapper.start(marcBibInstanceHridSetKafkaHandler, constructModuleName()),
        marcHoldingsConsumerWrapper.start(marcHoldingsRecordHridSetKafkaHandler, constructModuleName())
      )
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    CompositeFuture.join(marcBibConsumerWrapper.stop(), marcHoldingsConsumerWrapper.stop())
      .onComplete(ar -> stopPromise.complete());
  }

  private int getLoadLimit() {
    return Integer.parseInt(System.getProperty("inventory.kafka.MarcBibInstanceHridSetConsumer.loadLimit", "5"));
  }

  private KafkaConsumerWrapper<String, String> createConsumerByEvent(KafkaConfig kafkaConfig, DataImportEventTypes event) {
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(
      kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), event.value()
    );
    return KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(GLOBAL_LOAD_SENSOR)
      .subscriptionDefinition(subscriptionDefinition)
      .build();
  }
}
