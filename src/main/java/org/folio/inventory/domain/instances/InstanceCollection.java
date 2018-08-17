package org.folio.inventory.domain.instances;

import org.folio.inventory.domain.AsynchronousCollection;
import org.folio.inventory.domain.SearchableCollection;

public interface InstanceCollection
  extends AsynchronousCollection<Instance>, SearchableCollection<Instance> {
}
