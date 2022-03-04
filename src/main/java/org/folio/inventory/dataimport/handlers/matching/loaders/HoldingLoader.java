package org.folio.inventory.dataimport.handlers.matching.loaders;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.EntityType;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class HoldingLoader extends AbstractLoader<HoldingsRecord> {

  private static final String HOLDINGS_FIELD = "holdings";

  private Storage storage;

  public HoldingLoader(Storage storage, Vertx vertx) {
    super(vertx);
    this.storage = storage;
  }

  @Override
  protected EntityType getEntityType() {
    return EntityType.HOLDINGS;
  }

  @Override
  protected SearchableCollection<HoldingsRecord> getSearchableCollection(Context context) {
    return storage.getHoldingsRecordCollection(context);
  }

  @Override
  protected String addCqlSubMatchCondition(DataImportEventPayload eventPayload) {
    String cqlSubMatch = EMPTY;
    if (eventPayload.getContext() != null) {
      if (isNotEmpty(eventPayload.getContext().get(AbstractLoader.MULTI_MATCH_IDS))) {
        cqlSubMatch = getConditionByMultiMatchResult(eventPayload);
      } else if (isNotEmpty(eventPayload.getContext().get(EntityType.HOLDINGS.value()))) {
        JsonObject holdingAsJson = new JsonObject(eventPayload.getContext().get(EntityType.HOLDINGS.value()));
        if (holdingAsJson.getJsonObject(HOLDINGS_FIELD) != null) {
          holdingAsJson = holdingAsJson.getJsonObject(HOLDINGS_FIELD);
        }
        cqlSubMatch = format(" AND id == \"%s\"", holdingAsJson.getString("id"));
      } else if (isNotEmpty(eventPayload.getContext().get(EntityType.INSTANCE.value()))) {
        JsonObject instanceAsJson = new JsonObject(eventPayload.getContext().get(EntityType.INSTANCE.value()));
        cqlSubMatch = format(" AND instanceId == \"%s\"", instanceAsJson.getString("id"));
      }
    }
    return cqlSubMatch;
  }

  @Override
  protected String mapEntityToJsonString(HoldingsRecord holdingsRecord) {
    return Json.encode(holdingsRecord);
  }

  @Override
  protected String mapEntityListToIdsJson(List<HoldingsRecord> holdingList) {
    List<String> idList = holdingList.stream()
      .map(HoldingsRecord::getId)
      .collect(Collectors.toList());

    return Json.encode(idList);
  }
}
