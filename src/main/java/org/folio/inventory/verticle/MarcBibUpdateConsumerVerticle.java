package org.folio.inventory.verticle;

import static org.folio.inventory.dataimport.util.ConsumerWrapperUtil.constructModuleName;

import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.consumers.MarcBibUpdateKafkaConsumer;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.support.KafkaConsumerVerticle;

public class MarcBibUpdateConsumerVerticle extends KafkaConsumerVerticle {
  private static final Logger LOGGER = LogManager.getLogger(MarcBibUpdateConsumerVerticle.class);
  private static final String SRS_MARC_BIB_EVENT = "srs.marc-bib";
  private static final String BASE_PROPERTY = "MarcBibUpdateConsumer";

  @Override
  public void start(Promise<Void> startPromise) {
    var instanceUpdateDelegate = new InstanceUpdateDelegate(getStorage());

    var marcBibUpdateKafkaHandler = new MarcBibUpdateKafkaConsumer(vertx, getMaxDistributionNumber(BASE_PROPERTY),
      getKafkaConfig(), instanceUpdateDelegate);
    var marcBibUpdateConsumerWrapper = createConsumer(SRS_MARC_BIB_EVENT, BASE_PROPERTY, false);

    marcBibUpdateConsumerWrapper.start(marcBibUpdateKafkaHandler, constructModuleName())
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
