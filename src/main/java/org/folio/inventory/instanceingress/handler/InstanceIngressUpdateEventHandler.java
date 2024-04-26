package org.folio.inventory.instanceingress.handler;

import java.util.concurrent.CompletableFuture;
import org.folio.inventory.common.Context;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;

public class InstanceIngressUpdateEventHandler implements InstanceIngressEventHandler {

  public InstanceIngressUpdateEventHandler(Context context) {

  }

  @Override
  public CompletableFuture<InstanceIngressPayload> handle(InstanceIngressPayload payload) {
    return null;
  }
}
