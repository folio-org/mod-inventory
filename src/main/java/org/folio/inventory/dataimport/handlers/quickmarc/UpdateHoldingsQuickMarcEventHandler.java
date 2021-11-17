package org.folio.inventory.dataimport.handlers.quickmarc;

import java.util.Map;

import io.vertx.core.Promise;

import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.actions.HoldingsUpdateDelegate;
import org.folio.rest.jaxrs.model.Record;

public class UpdateHoldingsQuickMarcEventHandler extends AbstractQuickMarcEventHandler<HoldingsRecord> {

  private final Context context;
  private final HoldingsUpdateDelegate holdingsUpdateDelegate;

  public UpdateHoldingsQuickMarcEventHandler(HoldingsUpdateDelegate updateInstanceDelegate, Context context) {
    this.context = context;
    this.holdingsUpdateDelegate = updateInstanceDelegate;
  }

  @Override
  protected void updateEntity(Map<String, String> eventPayload, Record marcRecord, Promise<HoldingsRecord> handler) {
    holdingsUpdateDelegate.handle(eventPayload, marcRecord, context)
      .onSuccess(handler::complete)
      .onFailure(handler::fail);
  }

}
