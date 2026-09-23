package org.folio.inventory.verticle;

import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.consumers.MarcBibUpdateKafkaConsumer;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.support.KafkaConsumerVerticle;
import org.folio.kafka.OffsetResetStrategy;
import org.folio.kafka.services.ModuleIdResolver;

public class MarcBibUpdateConsumerVerticle extends KafkaConsumerVerticle {
  private static final Logger LOGGER = LogManager.getLogger(MarcBibUpdateConsumerVerticle.class);
  private static final String SRS_MARC_BIB_EVENT = "srs.marc-bib";
  private static final String BASE_PROPERTY = "MarcBibUpdateConsumer";

  @Override
  public void start(Promise<Void> startPromise) {
    var instanceUpdateDelegate = new InstanceUpdateDelegate(getStorage());

    var marcBibUpdateKafkaHandler = new MarcBibUpdateKafkaConsumer(vertx, getMaxDistributionNumber(BASE_PROPERTY),
      getKafkaConfig(), instanceUpdateDelegate);

    // POC - OffsetResetStrategy.EARLIEST starts consuming from the earliest available offset,
    //       including Kafka messages that were already in the topic before this module
    //       version was deployed.
    //
    // WARNING - Example scenario:
    //   T0 - mod-inventory-1.0.0 is deployed, and Tenant1 is entitled to this version.
    //
    //   T1 - 100,000 messages are published to the `srs.marc-bib` topic and processed by
    //        mod-inventory-1.0.0. These messages are retained in the topic for the next 8 hours.
    //
    //   T2 - mod-inventory-1.0.1 is deployed and started with a new consumer group.
    //        Since Kafka has no committed offset for this consumer group and
    //        autoOffsetReset=EARLIEST, consumption starts from the earliest available offset.
    //
    //        Messages for Tenant1 are discarded because Tenant1 is not yet entitled to
    //        mod-inventory-1.0.1. Assume 25,000 messages are consumed and discarded.
    //        The remaining 75,000 messages have not yet been consumed by this consumer group.
    //
    //   T3 - Tenant1 is upgraded to mod-inventory-1.0.1.
    //
    //   T4 - The remaining 75,000 messages are now consumed after Tenant1 is entitled to
    //        mod-inventory-1.0.1. Therefore, they are no longer discarded and will be processed.
    //        Since these messages were already processed by mod-inventory-1.0.0, processing them
    //        again by mod-inventory-1.0.1 could result in duplicate records in the database.
    var marcBibUpdateConsumerWrapper = createConsumer(SRS_MARC_BIB_EVENT, BASE_PROPERTY, false,
      OffsetResetStrategy.EARLIEST);

    var moduleId = ModuleIdResolver.resolve("mod-inventory");
    marcBibUpdateConsumerWrapper.start(marcBibUpdateKafkaHandler, moduleId, moduleId)
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
