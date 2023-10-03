package org.folio.inventory.consortium.handlers;

import io.vertx.core.Future;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.domain.instances.Instance;

import java.util.Map;

public interface InstanceSharingHandler {

  Future<String> publishInstance(Instance instance, SharingInstance sharingInstanceMetadata,
                                 Source source, Target target, Map<String, String> kafkaHeaders);

}
