package org.folio.inventory.domain;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.common.Context;

public interface SynchronousCollection<T> {

  T findByIdAndUpdate(String id, JsonObject entity, Context context) throws Exception;
}
