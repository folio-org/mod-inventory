package org.folio.inventory.dataimport.handlers.quickmarc;

import java.util.Map;

import io.vertx.core.Future;
import io.vertx.core.Promise;

import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.domain.instances.Instance;
import org.folio.rest.jaxrs.model.Record;

public class UpdateInstanceQuickMarcEventHandler extends AbstractQuickMarcEventHandler<Instance> {

  private final Context context;
  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;

  public UpdateInstanceQuickMarcEventHandler(InstanceUpdateDelegate updateInstanceDelegate, Context context,
                                             PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper) {
    this.context = context;
    this.instanceUpdateDelegate = updateInstanceDelegate;
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
  }

  @Override
  protected void updateEntity(Map<String, Object> eventPayload, Record marcRecord, Promise<Instance> handler) {
    Future<Instance> instanceUpdateFuture = instanceUpdateDelegate.handleMap(eventPayload, marcRecord, context);

    instanceUpdateFuture
      .compose(updatedInstance -> precedingSucceedingTitlesHelper.updatePrecedingSucceedingTitles(updatedInstance, context))
      .onComplete(ar -> {
        if (ar.succeeded()) {
          handler.complete(instanceUpdateFuture.result());
        } else {
          handler.fail(ar.cause());
        }
      });
  }

}
