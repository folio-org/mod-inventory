package org.folio.inventory.dataimport.services;

import io.vertx.core.Future;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;

public interface OrderHelperService {
  Future<Void> executeOrderLogicIfNeeded(DataImportEventPayload eventPayload, Context context);
}
