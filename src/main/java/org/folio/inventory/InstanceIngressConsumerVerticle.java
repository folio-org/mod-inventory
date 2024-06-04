package org.folio.inventory;

import static org.folio.inventory.dataimport.util.ConsumerWrapperUtil.constructModuleName;

import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.instanceingress.InstanceIngressEventConsumer;
import org.folio.inventory.support.KafkaConsumerVerticle;

public class InstanceIngressConsumerVerticle extends KafkaConsumerVerticle {

  private static final String INSTANCE_INGRESS_TOPIC = "inventory.instance_ingress";
  private static final Logger LOGGER = LogManager.getLogger(InstanceIngressConsumerVerticle.class);

  @Override
  public void start(Promise<Void> startPromise) {
    var instanceIngressEventHandler = new InstanceIngressEventConsumer(vertx, getStorage(), getHttpClient(), getMappingMetadataCache());

    //q remove namespace from a topic?
    var consumerWrapper = createConsumer(INSTANCE_INGRESS_TOPIC);

    consumerWrapper.start(instanceIngressEventHandler, constructModuleName())
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
