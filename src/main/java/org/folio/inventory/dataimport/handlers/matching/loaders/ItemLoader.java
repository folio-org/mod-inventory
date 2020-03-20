package org.folio.inventory.dataimport.handlers.matching.loaders;

import org.folio.inventory.common.Context;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.EntityType;

public class ItemLoader extends AbstractLoader<Item> {

  private Storage storage;

  public ItemLoader(Storage storage) {
    this.storage = storage;
  }

  @Override
  protected EntityType getEntityType() {
    return EntityType.ITEM;
  }

  @Override
  protected SearchableCollection<Item> getSearchableCollection(Context context) {
    return storage.getItemCollection(context);
  }
}
