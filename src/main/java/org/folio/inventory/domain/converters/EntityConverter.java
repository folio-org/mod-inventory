package org.folio.inventory.domain.converters;

import io.vertx.core.json.JsonObject;

public interface EntityConverter<T> {

  T fromJson(JsonObject json);

  JsonObject toJson(T entity);
}
