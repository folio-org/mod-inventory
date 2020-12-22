package org.folio.inventory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.inventory.dataimport.consumers.MarcBibInstanceHridSetKafkaHandler;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.util.pubsub.PubSubClientUtils;

import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;

public class MarcBibInstanceHridSetConsumerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(MarcBibInstanceHridSetConsumerVerticle.class);
  public static final String MARC_BIB_INSTANCE_HRID_SET_EVENT = "DI_SRS_MARC_BIB_INSTANCE_HRID_SET";
  private static final GlobalLoadSensor GLOBAL_LOAD_SENSOR = new GlobalLoadSensor();

  private int loadLimit = Integer.parseInt(System.getProperty("inventory.kafka.MarcBibInstanceHridSetConsumer.loadLimit", "5"));
  private KafkaConsumerWrapper<String, String> consumerWrapper;

  @Override
  public void start(Promise<Void> startPromise) {
    JsonObject config = vertx.getOrCreateContext().config();
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(config.getString(KAFKA_ENV))
      .kafkaHost(config.getString(KAFKA_HOST))
      .kafkaPort(config.getString(KAFKA_PORT))
      .okapiUrl(config.getString(OKAPI_URL))
      .replicationFactor(Integer.parseInt(config.getString(KAFKA_REPLICATION_FACTOR)))
      .build();
    LOGGER.debug("kafkaConfig: " + kafkaConfig);

    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(kafkaConfig.getEnvId(),
        KafkaTopicNameHelper.getDefaultNameSpace(), MARC_BIB_INSTANCE_HRID_SET_EVENT);

    consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(GLOBAL_LOAD_SENSOR)
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    HttpClient client = vertx.createHttpClient();
    Storage storage = Storage.basedUpon(vertx, config, client);
    InstanceUpdateDelegate instanceUpdateDelegate = new InstanceUpdateDelegate(storage);
    MarcBibInstanceHridSetKafkaHandler marcBibInstanceHridSetKafkaHandler = new MarcBibInstanceHridSetKafkaHandler(instanceUpdateDelegate);

    consumerWrapper.start(marcBibInstanceHridSetKafkaHandler, PubSubClientUtils.constructModuleName())
      .onSuccess(v -> startPromise.complete())
      .onFailure(startPromise::fail);
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumerWrapper.stop().onComplete(ar -> stopPromise.complete());
  }
}
