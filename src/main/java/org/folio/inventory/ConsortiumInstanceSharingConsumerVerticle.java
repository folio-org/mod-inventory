package org.folio.inventory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.consortium.consumers.ConsortiumInstanceSharingHandler;
import org.folio.inventory.consortium.model.ConsortiumEvenType;
import org.folio.inventory.dataimport.util.ConsumerWrapperUtil;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;

import java.util.ArrayList;
import java.util.List;

import static org.folio.inventory.consortium.model.ConsortiumEvenType.CONSORTIUM_INSTANCE_SHARING_INIT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;
import static org.folio.kafka.KafkaTopicNameHelper.createSubscriptionDefinition;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

public class ConsortiumInstanceSharingConsumerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger(DataImportConsumerVerticle.class);

  private List<KafkaConsumerWrapper<String, String>> consumerWrappers = new ArrayList<>();

  private final int loadLimit = getLoadLimit();

  private KafkaConsumerWrapper<String, String> consumer;

  @Override
  public void start(Promise<Void> startPromise) {
    JsonObject config = vertx.getOrCreateContext().config();
    KafkaConfig kafkaConfig = getKafkaConfig(config);

    HttpClient client = vertx.createHttpClient();
    Storage storage = Storage.basedUpon(config, vertx.createHttpClient());

    var handler = new ConsortiumInstanceSharingHandler(kafkaConfig, storage);
    var kafkaConsumerFuture = createKafkaConsumer(kafkaConfig, CONSORTIUM_INSTANCE_SHARING_INIT, handler);
    kafkaConsumerFuture.onSuccess(ar -> {
        consumer = ar;
        startPromise.complete();
      }).onFailure(startPromise::fail);
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumer.stop().onComplete(ar -> stopPromise.complete());
  }

  private Future<KafkaConsumerWrapper<String, String>> createKafkaConsumer(KafkaConfig kafkaConfig, ConsortiumEvenType eventType,
                                                                           AsyncRecordHandler<String, String> recordHandler) {
    var subscriptionDefinition =
      createSubscriptionDefinition(kafkaConfig.getEnvId(), getDefaultNameSpace(), eventType.name());

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

  private int getLoadLimit() {
    return Integer.parseInt(System.getProperty("inventory.kafka.ConsortiumInstanceSharingConsumer.loadLimit", "5"));
  }

}
