package org.folio.inventory.consortium.handlers;

import io.vertx.core.Future;
import java.util.Map;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.domain.instances.Instance;

public interface InstanceSharingHandler {

  Future<String> publishInstance(Instance instance, SharingInstance sharingInstanceMetadata,
                                 SourceTenantProvider sourceTenantProvider, TargetTenantProvider targetTenantProvider,
                                 Map<String, String> kafkaHeaders);
}
