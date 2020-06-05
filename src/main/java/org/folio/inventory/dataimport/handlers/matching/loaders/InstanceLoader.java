package org.folio.inventory.dataimport.handlers.matching.loaders;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.EntityType;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class InstanceLoader extends AbstractLoader<Instance> {

  private Storage storage;

  public InstanceLoader(Storage storage, Vertx vertx) {
    super(vertx);
    this.storage = storage;
  }

  @Override
  protected EntityType getEntityType() {
    return EntityType.INSTANCE;
  }

  @Override
  protected SearchableCollection<Instance> getSearchableCollection(Context context) {
    return storage.getInstanceCollection(context);
  }

  @Override
  protected String addCqlSubMatchCondition(DataImportEventPayload eventPayload) {
    String cqlSubMatch = EMPTY;
    if (eventPayload.getContext() != null && !isEmpty(eventPayload.getContext().get(EntityType.INSTANCE.value()))) {
      JsonObject instanceAsJson = new JsonObject(eventPayload.getContext().get(EntityType.INSTANCE.value()));
      cqlSubMatch = format(" AND id == \"%s\"", instanceAsJson.getString("id"));
    }
    return cqlSubMatch;
  }

  @Override
  protected String mapEntityToJsonString(Instance instance) {
    return Json.encode(instance);
  }
}
