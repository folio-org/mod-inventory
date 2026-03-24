package org.folio.inventory;

import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_INSTANCE_HRID_SET;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET;
import static org.folio.inventory.dataimport.util.ConsumerWrapperUtil.constructModuleName;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.consumers.MarcBibInstanceHridSetKafkaHandler;
import org.folio.inventory.dataimport.consumers.MarcHoldingsRecordHridSetKafkaHandler;
import org.folio.inventory.dataimport.handlers.actions.HoldingsUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.services.HoldingsCollectionService;
import org.folio.inventory.support.KafkaConsumerVerticle;

public class MarcHridSetConsumerVerticle extends KafkaConsumerVerticle {

  private static final Logger LOGGER = LogManager.getLogger(MarcHridSetConsumerVerticle.class);
  private static final String BASE_PROPERTY = "MarcBibInstanceHridSetConsumer";

  @Override
  public void start(Promise<Void> startPromise) {
    var marcBibConsumerWrapper = createConsumer(DI_SRS_MARC_BIB_INSTANCE_HRID_SET.value(), BASE_PROPERTY);
    var marcHoldingsConsumerWrapper = createConsumer(DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET.value(), BASE_PROPERTY);

    var holdingsCollectionService = new HoldingsCollectionService();
    var instanceUpdateDelegate = new InstanceUpdateDelegate(getStorage());
    var holdingsRecordUpdateDelegate = new HoldingsUpdateDelegate(getStorage(), holdingsCollectionService);

    var marcBibInstanceHridSetKafkaHandler = new MarcBibInstanceHridSetKafkaHandler(instanceUpdateDelegate, getMappingMetadataCache());
    var marcHoldingsRecordHridSetKafkaHandler = new MarcHoldingsRecordHridSetKafkaHandler(holdingsRecordUpdateDelegate, getMappingMetadataCache());

    Future.all(
        marcBibConsumerWrapper.start(marcBibInstanceHridSetKafkaHandler, constructModuleName()),
        marcHoldingsConsumerWrapper.start(marcHoldingsRecordHridSetKafkaHandler, constructModuleName())
      )
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

}
