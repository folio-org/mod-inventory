package org.folio.inventory.dataimport.handlers.matching.loaders;

import org.folio.inventory.common.Context;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.EntityType;

public class InstanceLoader extends AbstractLoader<Instance> {

  private Storage storage;

  public InstanceLoader(Storage storage) {
    this.storage = storage;
  }

  @Override
  EntityType getEntityType() {
    return EntityType.INSTANCE;
  }

  @Override
  SearchableCollection<Instance> getSearchableCollection(Context context) {
    return storage.getInstanceCollection(context);
  }

}
