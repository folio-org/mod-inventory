package org.folio.inventory;

import static org.folio.inventory.dataimport.util.ConsumerWrapperUtil.constructModuleName;

import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.handler.InstanceIngestEventHandler;
import org.folio.inventory.support.KafkaConsumerVerticle;

public class InstanceIngestConsumerVerticle extends KafkaConsumerVerticle {

  private static final String INSTANCE_INGEST_TOPIC = "inventory.instance_ingest";
  private static final Logger LOGGER = LogManager.getLogger(InstanceIngestConsumerVerticle.class);

  @Override
  public void start(Promise<Void> startPromise) {
    var instanceIngestEventHandler = new InstanceIngestEventHandler();

    var consumerWrapper = createConsumer(INSTANCE_INGEST_TOPIC);

    consumerWrapper.start(instanceIngestEventHandler, constructModuleName())
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
