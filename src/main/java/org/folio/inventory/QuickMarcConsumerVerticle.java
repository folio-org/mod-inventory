package org.folio.inventory;

import io.vertx.core.Promise;
import io.vertx.ext.web.client.WebClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.consumers.QuickMarcKafkaHandler;
import org.folio.inventory.dataimport.handlers.QMEventTypes;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.util.ConsumerWrapperUtil;
import org.folio.inventory.services.HoldingsCollectionService;
import org.folio.inventory.support.KafkaConsumerVerticle;

public class QuickMarcConsumerVerticle extends KafkaConsumerVerticle {

  private static final Logger LOGGER = LogManager.getLogger(QuickMarcConsumerVerticle.class);
  private static final String LOAD_LIMIT_PROPERTY = "QuickMarcConsumer";
  private static final String MAX_DISTRIBUTION_PROPERTY = "QuickMarcConsumerVerticle";

  @Override
  public void start(Promise<Void> startPromise) {
    var precedingSucceedingTitlesHelper = new PrecedingSucceedingTitlesHelper(WebClient.wrap(getHttpClient()));
    var holdingsCollectionService = new HoldingsCollectionService();
    var handler = new QuickMarcKafkaHandler(vertx, getStorage(), getMaxDistributionNumber(MAX_DISTRIBUTION_PROPERTY),
        getKafkaConfig(), precedingSucceedingTitlesHelper, holdingsCollectionService);

    var consumer = createConsumer(QMEventTypes.QM_SRS_MARC_RECORD_UPDATED.name(), LOAD_LIMIT_PROPERTY);

    consumer.start(handler, ConsumerWrapperUtil.constructModuleName())
      .map(consumer)
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

}
