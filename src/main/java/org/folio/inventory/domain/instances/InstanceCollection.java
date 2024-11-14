package org.folio.inventory.domain.instances;

import io.vertx.core.Future;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.AsynchronousCollection;
import org.folio.inventory.domain.SearchableCollection;

public interface InstanceCollection
  extends AsynchronousCollection<Instance>, SearchableCollection<Instance> {
  Future<Instance> findByIdAndUpdate(String id, Instance instance, Context context);
}
