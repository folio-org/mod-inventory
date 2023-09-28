package org.folio.inventory.consortium.handlers;

import io.vertx.core.Future;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;

import java.util.Map;

public interface InstanceSharingHandler {

  Future<String> publishInstance(Instance instance, SharingInstance sharingInstanceMetadata,
                                 InstanceCollection targetInstanceCollection,
                                 InstanceCollection sourceInstanceCollection,
                                 Map<String, String> kafkaHeaders);

}
