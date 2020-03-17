package org.folio.inventory.dataimport.handlers.matching.loaders;

import org.folio.inventory.common.Context;
import org.folio.inventory.domain.Holding;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.EntityType;

public class HoldingLoader extends AbstractLoader<Holding> {

  private Storage storage;

  public HoldingLoader(Storage storage) {
    this.storage = storage;
  }

  @Override
  EntityType getEntityType() {
    return EntityType.HOLDINGS;
  }

  @Override
  SearchableCollection<Holding> getSearchableCollection(Context context) {
    return storage.getHoldingCollection(context);
  }
}
