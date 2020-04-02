package org.folio.inventory.dataimport.handlers.matching.loaders;

import io.vertx.core.WorkerExecutor;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.EntityType;

public class HoldingLoader extends AbstractLoader<HoldingsRecord> {

  private Storage storage;

  public HoldingLoader(Storage storage, WorkerExecutor executor) {
    super(executor);
    this.storage = storage;
  }

  @Override
  protected EntityType getEntityType() {
    return EntityType.HOLDINGS;
  }

  @Override
  protected SearchableCollection<HoldingsRecord> getSearchableCollection(Context context) {
    return storage.getHoldingsRecordCollection(context);
  }
}
