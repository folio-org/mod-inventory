package org.folio.inventory.dataimport.services;

import io.vertx.core.Future;
import org.folio.DataImportEventPayload;
import org.folio.DataImportEventTypes;
import org.folio.inventory.common.Context;

/**
 * Service for processing logic for Order's mechanism.
 */
public interface OrderHelperService {
  /**
   * Check if there is an Order CREATE action profile in snapshotProfileWrapper-tree, and if the current action profile is the last one.
   * If true, then filling dataImportEventPayload via data: 1. Into context add an event to the eventChain based on entity-type.; 2. Set eventType "DI_ORDER_CREATED_READY_FOR_POST_PROCESSING".
   *
   * @param eventPayload               - DataImportEventPayload for current import.
   * @param targetEventType - event type which should be filled inside payload (provided from Handlers based on entity-type)
   * @param context                    - Context for retrieving jobProfileSnapshotWrapper from cache.
   * @return -Future<Void>
   */
  Future<Void> fillPayloadForOrderPostProcessingIfNeeded(DataImportEventPayload eventPayload, DataImportEventTypes targetEventType, Context context);
}
