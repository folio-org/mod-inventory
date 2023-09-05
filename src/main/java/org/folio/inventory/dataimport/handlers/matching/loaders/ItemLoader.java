package org.folio.inventory.dataimport.handlers.matching.loaders;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.preloaders.AbstractPreloader;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.ItemUtil;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.rest.jaxrs.model.EntityType;

public class ItemLoader extends AbstractLoader<Item> {

  private static final String HOLDINGS_FIELD = "holdings";

  private Storage storage;
  private AbstractPreloader preloader;

  public ItemLoader(Storage storage, Vertx vertx, AbstractPreloader preloader) {
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
      if (isNotEmpty(eventPayload.getContext().get(AbstractLoader.MULTI_MATCH_IDS))) {
        cqlSubMatch = getConditionByMultiMatchResult(eventPayload);
      } else if (isNotEmpty(eventPayload.getContext().get(EntityType.ITEM.value()))) {
        JsonObject itemAsJson = new JsonObject(eventPayload.getContext().get(EntityType.ITEM.value()));
        cqlSubMatch = format(" AND id == \"%s\"", itemAsJson.getString("id"));
      } else if (isNotEmpty(eventPayload.getContext().get(EntityType.HOLDINGS.value()))) {
        JsonArray holdingsAsJson = new JsonArray(eventPayload.getContext().get(EntityType.HOLDINGS.value()));
        if (!holdingsAsJson.isEmpty()) {
          String holdingIds = holdingsAsJson.stream().map(JsonObject.class::cast)
            .map(jsonObj -> {
              if (jsonObj.getJsonObject(HOLDINGS_FIELD) != null) {
                return jsonObj.getJsonObject(HOLDINGS_FIELD).getString("id");
              }
              return jsonObj.getString("id");
            }).collect(Collectors.joining(" OR "));
          cqlSubMatch = format(" AND holdingsRecordId == (%s)", holdingIds);
        }
      }
    }
    return cqlSubMatch;
  }

  @Override
  protected String mapEntityToJsonString(Item item) {
    return ItemUtil.mapToMappingResultRepresentation(item);
  }

  @Override
  protected String mapEntityListToIdsJsonString(List<Item> itemList) {
    List<String> idList = itemList.stream()
      .map(Item::getId)
      .collect(Collectors.toList());

    return Json.encode(idList);
  }
}
