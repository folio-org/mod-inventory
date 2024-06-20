package org.folio.inventory.instanceingress.handler;

import java.util.concurrent.CompletableFuture;
import org.folio.inventory.domain.instances.Instance;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;

public interface InstanceIngressEventHandler {

  CompletableFuture<Instance> handle(InstanceIngressEvent instanceIngressEvent);

}
