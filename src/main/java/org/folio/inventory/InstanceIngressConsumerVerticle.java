package org.folio.inventory;

import static org.folio.inventory.dataimport.util.ConsumerWrapperUtil.constructModuleName;

import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.instanceingress.InstanceIngressEventConsumer;
import org.folio.inventory.support.KafkaConsumerVerticle;

public class InstanceIngressConsumerVerticle extends KafkaConsumerVerticle {

  private static final Logger LOGGER = LogManager.getLogger(InstanceIngressConsumerVerticle.class);
  private static final String INSTANCE_INGRESS_TOPIC = "linked-data.instance-ingress";
  private static final String BASE_PROPERTY = "InstanceIngressConsumerVerticle";

  @Override
  public void start(Promise<Void> startPromise) {
    var instanceIngressEventHandler = new InstanceIngressEventConsumer(vertx, getStorage(), getHttpClient(), getMappingMetadataCache());

    var consumerWrapper = createConsumer(INSTANCE_INGRESS_TOPIC, BASE_PROPERTY, false);

    consumerWrapper.start(instanceIngressEventHandler, constructModuleName())
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

}
