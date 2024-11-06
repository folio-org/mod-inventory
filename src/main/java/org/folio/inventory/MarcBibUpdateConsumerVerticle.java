package org.folio.inventory;

import static org.folio.inventory.dataimport.util.ConsumerWrapperUtil.constructModuleName;

import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.consumers.MarcBibUpdateKafkaHandler;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.support.KafkaConsumerVerticle;

public class MarcBibUpdateConsumerVerticle extends KafkaConsumerVerticle {
  private static final Logger LOGGER = LogManager.getLogger(MarcBibUpdateConsumerVerticle.class);
  private static final String SRS_MARC_BIB_EVENT = "srs.marc-bib";
  private static final String BASE_PROPERTY = "MarcBibUpdateConsumer";
  private static final String DEFAULT_LOAD_LIMIT = "2";

  @Override
  public void start(Promise<Void> startPromise) {
    var instanceUpdateDelegate = new InstanceUpdateDelegate(getStorage());

    var marcBibUpdateKafkaHandler = new MarcBibUpdateKafkaHandler(vertx, getMaxDistributionNumber(BASE_PROPERTY),
      getKafkaConfig(), instanceUpdateDelegate, getMappingMetadataCache());
    var loadLimit = getLoadLimit(BASE_PROPERTY, DEFAULT_LOAD_LIMIT);
    var marcBibUpdateConsumerWrapper = createConsumer(SRS_MARC_BIB_EVENT, loadLimit, false);

    marcBibUpdateConsumerWrapper.start(marcBibUpdateKafkaHandler, constructModuleName())
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

}
