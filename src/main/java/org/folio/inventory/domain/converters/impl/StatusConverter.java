package org.folio.inventory.domain.converters.impl;

import static org.folio.inventory.support.JsonHelper.getString;
import static org.folio.inventory.support.JsonHelper.includeIfPresent;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.converters.EntityConverter;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;

public class StatusConverter implements EntityConverter<Status> {
  private static final String NAME_KEY = "name";
  private static final String DATE_KEY = "date";

  @Override
  public Status fromJson(JsonObject json) {
    return new Status(ItemStatusName.forName(getString(json, NAME_KEY)),
      getString(json, DATE_KEY));
  }

  @Override
  public JsonObject toJson(Status entity) {
    JsonObject status = new JsonObject();

    if (entity.name() != null) {
      status.put(NAME_KEY, entity.name().value());
    }
    includeIfPresent(status, DATE_KEY, entity.date());

    return status;
  }
}
