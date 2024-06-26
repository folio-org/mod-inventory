package org.folio.inventory.instanceingress.handler;

import java.util.concurrent.CompletableFuture;
import org.folio.inventory.domain.instances.Instance;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;

public class InstanceIngressUpdateEventHandler implements InstanceIngressEventHandler {

  @Override
  public CompletableFuture<Instance> handle(InstanceIngressEvent instanceIngressEvent) {
    // to be implemented in MODINV-1008
    return CompletableFuture.failedFuture(new UnsupportedOperationException());
  }
}
