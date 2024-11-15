package org.folio.inventory.domain.instances;

import io.vertx.core.Future;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.AsynchronousCollection;
import org.folio.inventory.domain.SearchableCollection;

public interface InstanceCollection
  extends AsynchronousCollection<Instance>, SearchableCollection<Instance> {
  Future<String> findByIdAndUpdate(String id, org.folio.Instance mappedInstance, Context context);

  Future<Instance> findByIdAndUpdate(String id, org.folio.Instance mappedInstance);
}
