package org.folio.inventory.domain.instances;

import org.folio.inventory.domain.AsynchronousCollection;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.domain.SynchronousCollection;

public interface InstanceCollection extends AsynchronousCollection<Instance>,
  SearchableCollection<Instance>, SynchronousCollection<Instance> {

}
