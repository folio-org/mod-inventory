package org.folio.inventory;

import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.DataImportEventTypes.DI_MARC_FOR_UPDATE_RECEIVED;
import static org.folio.DataImportEventTypes.DI_PENDING_ORDER_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_DELETED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;

import io.vertx.core.Promise;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventTypes;
import org.folio.inventory.consortium.cache.ConsortiumDataCache;
import org.folio.inventory.dataimport.cache.CancelledJobsIdsCache;
import org.folio.inventory.dataimport.consumers.DataImportKafkaHandler;
import org.folio.inventory.dataimport.util.ConsumerWrapperUtil;
import org.folio.inventory.support.KafkaConsumerVerticle;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.processing.events.EventManager;

public class DataImportConsumerVerticle extends KafkaConsumerVerticle {

  private static final Logger LOGGER = LogManager.getLogger(DataImportConsumerVerticle.class);

  private static final List<DataImportEventTypes> EVENT_TYPES = List.of(
    DI_INVENTORY_HOLDING_CREATED,
    DI_INVENTORY_HOLDING_MATCHED,
    DI_INVENTORY_HOLDING_NOT_MATCHED,
    DI_INVENTORY_HOLDING_UPDATED,
    DI_INVENTORY_INSTANCE_CREATED,
    DI_INVENTORY_INSTANCE_MATCHED,
    DI_INVENTORY_INSTANCE_NOT_MATCHED,
    DI_INVENTORY_INSTANCE_UPDATED,
    DI_INVENTORY_ITEM_CREATED,
    DI_INVENTORY_ITEM_UPDATED,
    DI_INVENTORY_ITEM_MATCHED,
    DI_INVENTORY_ITEM_NOT_MATCHED,
    DI_MARC_FOR_UPDATE_RECEIVED,
    DI_SRS_MARC_AUTHORITY_RECORD_CREATED,
    DI_SRS_MARC_AUTHORITY_RECORD_DELETED,
    DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING,
    DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED,
    DI_INCOMING_MARC_BIB_RECORD_PARSED,
    DI_SRS_MARC_BIB_RECORD_UPDATED,
    DI_SRS_MARC_BIB_RECORD_MATCHED,
    DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING,
    DI_SRS_MARC_BIB_RECORD_MODIFIED,
    DI_SRS_MARC_BIB_RECORD_NOT_MATCHED,
    DI_SRS_MARC_HOLDING_RECORD_CREATED,
    DI_SRS_MARC_HOLDINGS_RECORD_MODIFIED_READY_FOR_POST_PROCESSING,
    DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED,
    DI_PENDING_ORDER_CREATED
  );
  private static final String LOAD_LIMIT_PROPERTY = "DataImportConsumer";
  private static final String MAX_DISTRIBUTION_PROPERTY = "DataImportConsumerVerticle";

  private final CancelledJobsIdsCache cancelledJobsIdsCache;

  public DataImportConsumerVerticle(CancelledJobsIdsCache cancelledJobsIdsCache) {
    this.cancelledJobsIdsCache = cancelledJobsIdsCache;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    EventManager.registerKafkaEventPublisher(getKafkaConfig(), vertx, getMaxDistributionNumber(MAX_DISTRIBUTION_PROPERTY));
    var consortiumDataCache = new ConsortiumDataCache(vertx, getHttpClient());

    var dataImportKafkaHandler = new DataImportKafkaHandler(vertx, getStorage(), getHttpClient(), getProfileSnapshotCache(),
      getKafkaConfig(), getMappingMetadataCache(), consortiumDataCache, cancelledJobsIdsCache);

    var futures = EVENT_TYPES.stream()
      .map(type -> super.createConsumer(type.value(), LOAD_LIMIT_PROPERTY))
      .map(consumerWrapper -> consumerWrapper.start(dataImportKafkaHandler, ConsumerWrapperUtil.constructModuleName())
        .map(consumerWrapper)
      )
      .toList();

    GenericCompositeFuture.all(futures)
      .onFailure(startPromise::fail)
      .onSuccess(ar -> startPromise.complete());
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

}
