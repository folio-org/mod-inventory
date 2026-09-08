package org.folio.inventory.consortium.handlers;

import org.folio.inventory.domain.instances.InstanceCollection;

public record TargetTenantProvider(String tenantId, InstanceCollection instanceCollection) implements TenantProvider { }
