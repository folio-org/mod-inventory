package org.folio.inventory.dataimport.handlers.matching.loaders;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.preloaders.AbstractPreloader;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.storage.Storage;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.rest.jaxrs.model.EntityType;

public class InstanceLoader extends AbstractLoader<Instance> {

  private static final String INSTANCES_IDS_KEY = "INSTANCES_IDS";
  private static final String ID_FIELD = "id";

  private Storage storage;
  private AbstractPreloader preloader;

  public InstanceLoader(Storage storage, Vertx vertx, AbstractPreloader preloader) {
    super(vertx);
    this.storage = storage;
    this.preloader = preloader;
  }

  @Override
  public CompletableFuture<LoadResult> loadEntity(LoadQuery loadQuery, DataImportEventPayload eventPayload) {
    return preloader.preload(loadQuery, eventPayload)
            .thenCompose(query -> super.loadEntity(query, eventPayload));
  }

  @Override
  protected EntityType getEntityType() {
    return INSTANCE;
  }

  @Override
  protected SearchableCollection<Instance> getSearchableCollection(Context context) {
    return storage.getInstanceCollection(context);
  }

  @Override
  protected String addCqlSubMatchCondition(DataImportEventPayload eventPayload) {
    String cqlSubMatch = EMPTY;
    if (eventPayload.getContext() != null) {
      if (isNotEmpty(eventPayload.getContext().get(AbstractLoader.MULTI_MATCH_IDS))
        || isNotEmpty(eventPayload.getContext().get(INSTANCES_IDS_KEY))) {
        cqlSubMatch = getConditionByMultiMatchResult(eventPayload);
      } else if (isNotEmpty(eventPayload.getContext().get(INSTANCE.value()))) {
        JsonObject instanceAsJson = new JsonObject(eventPayload.getContext().get(INSTANCE.value()));
        cqlSubMatch = format(" AND id == \"%s\"", instanceAsJson.getString(ID_FIELD));
      }
    }
    return cqlSubMatch;
  }

  @Override
  protected String getConditionByMultiMatchResult(DataImportEventPayload eventPayload) {
    String multipleValuesKey = eventPayload.getContext().containsKey(AbstractLoader.MULTI_MATCH_IDS)
      ? AbstractLoader.MULTI_MATCH_IDS
      : INSTANCES_IDS_KEY;

    return getConditionByMultipleValues(ID_FIELD, eventPayload, multipleValuesKey);
  }

  @Override
  protected String mapEntityToJsonString(Instance instance) {
    return Json.encode(instance);
  }

  @Override
  protected String mapEntityListToIdsJsonString(List<Instance> instanceList) {
    List<String> idList = instanceList.stream()
      .map(Instance::getId)
      .collect(Collectors.toList());

    return Json.encode(idList);
  }
}
