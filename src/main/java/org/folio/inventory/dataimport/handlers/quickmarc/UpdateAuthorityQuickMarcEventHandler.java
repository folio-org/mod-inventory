package org.folio.inventory.dataimport.handlers.quickmarc;

import java.util.HashMap;
import java.util.Map;

import io.vertx.core.Promise;

import org.folio.Authority;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.actions.AuthorityUpdateDelegate;
import org.folio.rest.jaxrs.model.Record;

public class UpdateAuthorityQuickMarcEventHandler extends AbstractQuickMarcEventHandler<Authority> {

  private final Context context;
  private final AuthorityUpdateDelegate authorityUpdateDelegate;

  public UpdateAuthorityQuickMarcEventHandler(AuthorityUpdateDelegate authorityUpdateDelegate, Context context) {
    this.context = context;
    this.authorityUpdateDelegate = authorityUpdateDelegate;
  }

  @Override
  protected void updateEntity(Map<String, Object> eventPayload, Record marcRecord, Promise<Authority> handler) {
    authorityUpdateDelegate.handle(new HashMap<>(), marcRecord, context)
      .onSuccess(handler::complete)
      .onFailure(handler::fail);
  }

}
