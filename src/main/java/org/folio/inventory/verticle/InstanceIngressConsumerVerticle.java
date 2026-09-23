package org.folio.inventory.verticle;

import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.instanceingress.InstanceIngressEventConsumer;
import org.folio.inventory.support.KafkaConsumerVerticle;
import org.folio.kafka.OffsetResetStrategy;
import org.folio.kafka.services.ModuleIdResolver;

public class InstanceIngressConsumerVerticle extends KafkaConsumerVerticle {

  public static final String INSTANCE_INGRESS_TOPIC = "inventory.instance_ingress";

  private static final Logger LOGGER = LogManager.getLogger(InstanceIngressConsumerVerticle.class);
  private static final String BASE_PROPERTY = "InstanceIngressConsumerVerticle";

  @Override
  public void start(Promise<Void> startPromise) {
    var instanceIngressEventHandler = new InstanceIngressEventConsumer(vertx, getStorage(), getHttpClient());

    // POC - OffsetResetStrategy.LATEST = start from the latest offset and do
    //       not read the messages that were already in the topic before this
    //       version was deployed.
    var consumerWrapper = createConsumer(INSTANCE_INGRESS_TOPIC, BASE_PROPERTY, OffsetResetStrategy.LATEST);

    // POC - Resolve the same module id that is used by mgr-tenant-entitlements
    //       and sidecar for this module.
    //       eg: mod-inventory-1.0.0-SNAPSHOT.1021
    var moduleId = ModuleIdResolver.resolve("mod-inventory");

    // POC - Use the three-argument version of start()
    consumerWrapper.start(instanceIngressEventHandler, moduleId, moduleId)
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
