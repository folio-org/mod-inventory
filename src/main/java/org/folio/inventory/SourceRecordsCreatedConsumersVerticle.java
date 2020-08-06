package org.folio.inventory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import org.folio.inventory.kafka.AsyncRecordHandler;
import org.folio.inventory.kafka.GlobalLoadSensor;
import org.folio.inventory.kafka.KafkaConfig;
import org.folio.inventory.kafka.KafkaConsumerWrapper;
import org.folio.inventory.kafka.KafkaTopicNameHelper;
import org.folio.inventory.kafka.SubscriptionDefinition;

public class SourceRecordsCreatedConsumersVerticle extends AbstractVerticle {

  private AsyncRecordHandler<String, String> sourceRecordsCreatedKafkaHandler;
  private KafkaConfig kafkaConfig;
  private final int loadLimit = 10;

  private GlobalLoadSensor globalLoadSensor = new GlobalLoadSensor();

  private KafkaConsumerWrapper<String, String> consumerWrapper;

  public SourceRecordsCreatedConsumersVerticle(AsyncRecordHandler<String, String> sourceRecordsCreatedKafkaHandler, KafkaConfig kafkaConfig) {
    super();
    this.sourceRecordsCreatedKafkaHandler = sourceRecordsCreatedKafkaHandler;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public void start(Future<Void> startPromise) {

    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(), "DI_SRS_MARC_BIB_RECORD_CREATED");

    consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(globalLoadSensor)
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    consumerWrapper.start(sourceRecordsCreatedKafkaHandler).setHandler(sar -> {
      if (sar.succeeded()) {
        startPromise.complete();
      } else {
        startPromise.fail(sar.cause());
      }
    });
  }

  @Override
  public void stop(Future<Void> stopPromise) {
    consumerWrapper.stop().setHandler(ar -> stopPromise.complete());
  }

}
