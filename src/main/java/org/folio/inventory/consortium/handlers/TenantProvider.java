package org.folio.inventory.consortium.handlers;

import org.folio.inventory.domain.instances.InstanceCollection;

public interface TenantProvider {
  String getTenantId();
  InstanceCollection getInstanceCollection();
}
