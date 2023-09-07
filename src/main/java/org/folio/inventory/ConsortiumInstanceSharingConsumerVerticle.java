package org.folio.inventory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.consortium.consumers.ConsortiumInstanceSharingHandler;
import org.folio.inventory.consortium.model.SharingInstanceEventType;
import org.folio.inventory.dataimport.util.ConsumerWrapperUtil;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;

import static java.lang.String.format;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;

public class ConsortiumInstanceSharingConsumerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger(ConsortiumInstanceSharingConsumerVerticle.class);

  private final int loadLimit = getLoadLimit();

  private KafkaConsumerWrapper<String, String> consumer;

  @Override
  public void start(Promise<Void> startPromise) {
    JsonObject config = vertx.getOrCreateContext().config();
    KafkaConfig kafkaConfig = getKafkaConfig(config);
    LOGGER.info(format("kafkaConfig: %s", kafkaConfig));

    Storage storage = Storage.basedUpon(config, vertx.createHttpClient());
    ConsortiumInstanceSharingHandler consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertx, storage, kafkaConfig);

    var kafkaConsumerFuture = createKafkaConsumerWrapper(kafkaConfig, consortiumInstanceSharingHandler);
    kafkaConsumerFuture.onFailure(startPromise::fail)
      .onSuccess(ar -> {
        consumer = ar;
        startPromise.complete();
      });
  }

  private Future<KafkaConsumerWrapper<String, String>> createKafkaConsumerWrapper(KafkaConfig kafkaConfig,
                                                                                  AsyncRecordHandler<String, String> recordHandler) {
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), SharingInstanceEventType.CONSORTIUM_INSTANCE_SHARING_INIT.value());

    KafkaConsumerWrapper<String, String> consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    return consumerWrapper
      .start(recordHandler, ConsumerWrapperUtil.constructModuleName())
      .map(consumerWrapper);
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

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumer.stop().onComplete(ar -> stopPromise.complete());
  }

  private int getLoadLimit() {
    return Integer.parseInt(System.getProperty("inventory.kafka.ConsortiumInstanceSharingConsumer.loadLimit", "5"));
  }

}
