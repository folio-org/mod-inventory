package org.folio.inventory.dataimport.services;

import io.vertx.core.Future;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;

/**
 * Service for processing logic for Order's mechanism.
 */
public interface OrderHelperService {
  /**
   * Check if there is an Order CREATE action profile in snapshotProfileWrapper-tree, and if the current action profile is the last one.
   * If true, then send an "DI_ORDER_READY_FOR_POST_PROCESSING"-event to Kafka with the currentPayload.
   * @param eventPayload - DataImportEventPayload for current import.
   * @param context - Context for sending event to Kafka
   * @return -Future<Void>
   */
  Future<Void> sendOrderPostProcessingEventIfNeeded(DataImportEventPayload eventPayload, Context context);
}
