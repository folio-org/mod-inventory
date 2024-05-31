package org.folio.inventory.instanceingress.handler;

import java.util.concurrent.CompletableFuture;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;

public interface InstanceIngressEventHandler {

  CompletableFuture<InstanceIngressEvent> handle(InstanceIngressEvent instanceIngressEvent);

}
