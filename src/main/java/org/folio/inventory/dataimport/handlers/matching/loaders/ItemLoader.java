package org.folio.inventory.dataimport.handlers.matching.loaders;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.EntityType;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class ItemLoader extends AbstractLoader<Item> {

  private static final String HOLDINGS_FIELD = "holdings";

  private Storage storage;

  public ItemLoader(Storage storage, Vertx vertx) {
    super(vertx);
    this.storage = storage;
  }

  @Override
  protected EntityType getEntityType() {
    return EntityType.ITEM;
  }

  @Override
  protected SearchableCollection<Item> getSearchableCollection(Context context) {
    return storage.getItemCollection(context);
  }

  @Override
  protected String addCqlSubMatchCondition(DataImportEventPayload eventPayload) {
    String cqlSubMatch = EMPTY;
    if (eventPayload.getContext() != null) {
      if (!isEmpty(eventPayload.getContext().get(EntityType.ITEM.value()))) {
        JsonObject itemAsJson = new JsonObject(eventPayload.getContext().get(EntityType.ITEM.value()));
        cqlSubMatch = format(" AND id == \"%s\"", itemAsJson.getString("id"));
      } else if (!isEmpty(eventPayload.getContext().get(EntityType.HOLDINGS.value()))) {
        JsonObject holdingAsJson = new JsonObject(eventPayload.getContext().get(EntityType.HOLDINGS.value()));
        if (holdingAsJson.getJsonObject(HOLDINGS_FIELD) != null) {
          holdingAsJson = holdingAsJson.getJsonObject(HOLDINGS_FIELD);
        }
        cqlSubMatch = format(" AND holdingsRecordId == \"%s\"", holdingAsJson.getString("id"));
      }
    }
    return cqlSubMatch;
  }
}
