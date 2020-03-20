package org.folio.inventory.dataimport.handlers.matching.loaders;

import org.folio.Holdingsrecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.EntityType;

public class HoldingLoader extends AbstractLoader<Holdingsrecord> {

  private Storage storage;

  public HoldingLoader(Storage storage) {
    this.storage = storage;
  }

  @Override
  EntityType getEntityType() {
    return EntityType.HOLDINGS;
  }

  @Override
  SearchableCollection<Holdingsrecord> getSearchableCollection(Context context) {
    return storage.getHoldingsRecordCollection(context);
  }
}
