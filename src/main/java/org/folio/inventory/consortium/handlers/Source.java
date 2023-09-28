package org.folio.inventory.consortium.handlers;

import org.folio.inventory.domain.instances.InstanceCollection;

public class Source implements TenantProvider {
  private String tenantId;
  private final InstanceCollection instanceCollection;

  public Source(String tenantId, InstanceCollection instanceCollection) {
    this.tenantId = tenantId;
    this.instanceCollection = instanceCollection;
  }

  public String getTenantId() {
    return tenantId;
  }

  public InstanceCollection getInstanceCollection() {
    return instanceCollection;
  }
}
