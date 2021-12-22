package org.folio.inventory.dataimport.handlers.matching.loaders;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.folio.Authority;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.EntityType;

import static org.apache.commons.lang3.StringUtils.EMPTY;

public class AuthorityLoader extends AbstractLoader<Authority> {
  private static final String FIELD = "authority";

  private Storage storage;

  public AuthorityLoader(Storage storage, Vertx vertx) {
    super(vertx);
    this.storage = storage;
  }

  @Override
  protected EntityType getEntityType() {
    return EntityType.AUTHORITY;
  }

  @Override
  protected SearchableCollection<Authority> getSearchableCollection(Context context) {
    return storage.getAuthorityRecordCollection(context);
  }

  @Override
  protected String addCqlSubMatchCondition(DataImportEventPayload eventPayload) {
    String cqlSubMatch = EMPTY;
    if (eventPayload.getContext() != null) {
      if (!StringUtils.isEmpty(eventPayload.getContext().get(EntityType.AUTHORITY.value()))) {
        JsonObject authorityAsJson = new JsonObject(eventPayload.getContext().get(EntityType.AUTHORITY.value()));
        if (authorityAsJson.getJsonObject(FIELD) != null) {
          authorityAsJson = authorityAsJson.getJsonObject(FIELD);
        }
        cqlSubMatch = String.format(" AND id == \"%s\"", authorityAsJson.getString("id"));
      }
    }
    return cqlSubMatch;
  }

  @Override
  protected String mapEntityToJsonString(Authority authority) {
    return Json.encode(authority);
  }
}
