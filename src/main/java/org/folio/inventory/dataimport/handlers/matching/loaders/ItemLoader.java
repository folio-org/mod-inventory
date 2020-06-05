package org.folio.inventory.dataimport.handlers.matching.loaders;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.EntityType;

import java.util.UUID;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.inventory.domain.converters.EntityConverters.converterForClass;
import static org.folio.inventory.support.JsonHelper.includeIfPresent;

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

  @Override
  protected String mapEntityToJsonString(Item item) {
    JsonObject itemJson = new JsonObject();
    itemJson.put("id", item.id != null
      ? item.id
      : UUID.randomUUID().toString());

    itemJson.put("status", converterForClass(Status.class).toJson(item.getStatus()));

    if(item.getLastCheckIn() != null) {
      itemJson.put(Item.LAST_CHECK_IN, item.getLastCheckIn().toJson());
    }

    includeIfPresent(itemJson, Item.HRID_KEY, item.getHrid());
    itemJson.put(Item.FORMER_IDS_KEY, item.getFormerIds());
    itemJson.put(Item.DISCOVERY_SUPPRESS_KEY, item.getDiscoverySuppress());
    includeIfPresent(itemJson, "copyNumber", item.getCopyNumber());
    itemJson.put("notes", item.getNotes());
    itemJson.put(Item.CIRCULATION_NOTES_KEY, item.getCirculationNotes());
    includeIfPresent(itemJson, "barcode", item.getBarcode());
    includeIfPresent(itemJson, Item.ITEM_LEVEL_CALL_NUMBER_KEY, item.getItemLevelCallNumber());
    includeIfPresent(itemJson, Item.ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY, item.getItemLevelCallNumberPrefix());
    includeIfPresent(itemJson, Item.ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY, item.getItemLevelCallNumberSuffix());
    includeIfPresent(itemJson, Item.ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY, item.getItemLevelCallNumberTypeId());
    includeIfPresent(itemJson, Item.VOLUME_KEY, item.getVolume());
    includeIfPresent(itemJson, "enumeration", item.getEnumeration());
    includeIfPresent(itemJson, "chronology", item.getChronology());
    includeIfPresent(itemJson, "numberOfPieces", item.getNumberOfPieces());
    includeIfPresent(itemJson, Item.DESCRIPTION_OF_PIECES_KEY, item.getDescriptionOfPieces());
    includeIfPresent(itemJson, Item.NUMBER_OF_MISSING_PIECES_KEY, item.getNumberOfMissingPieces());
    includeIfPresent(itemJson, Item.MISSING_PIECES_KEY, item.getMissingPieces());
    includeIfPresent(itemJson, Item.MISSING_PIECES_DATE_KEY, item.getMissingPiecesDate());
    includeIfPresent(itemJson, Item.ITEM_DAMAGED_STATUS_ID_KEY, item.getItemDamagedStatusId());
    includeIfPresent(itemJson, Item.ITEM_DAMAGED_STATUS_DATE_KEY, item.getItemDamagedStatusDate());
    includeIfPresent(itemJson, "holdingsRecordId", item.getHoldingId());
    includeIfPresent(itemJson, Item.ACCESSION_NUMBER_KEY, item.getAccessionNumber());
    includeIfPresent(itemJson, Item.ITEM_IDENTIFIER_KEY, item.getItemIdentifier());

    if (item.getMaterialTypeId() != null) {
      itemJson.put("materialType", new JsonObject().put("id", item.getMaterialTypeId()));
    }
    if (item.getPermanentLoanTypeId() != null) {
      itemJson.put("permanentLoanType", new JsonObject().put("id", item.getPermanentLoanTypeId()));
    }
    if (item.getTemporaryLoanTypeId() != null) {
      itemJson.put("temporaryLoanType", new JsonObject().put("id", item.getTemporaryLoanTypeId()));
    }
    if (item.getPermanentLocationId() != null) {
      itemJson.put("permanentLocation", new JsonObject().put("id", item.getPermanentLocationId()));
    }
    if (item.getTemporaryLocationId() != null) {
      itemJson.put("temporaryLocation", new JsonObject().put("id", item.getPermanentLocationId()));
    }

    itemJson.put(Item.YEAR_CAPTION_KEY, item.getYearCaption());
    itemJson.put(Item.ELECTRONIC_ACCESS_KEY, item.getElectronicAccess());
    itemJson.put(Item.STATISTICAL_CODE_IDS_KEY, item.getStatisticalCodeIds());
    itemJson.put(Item.PURCHASE_ORDER_LINE_IDENTIFIER, item.getPurchaseOrderLineidentifier());
    itemJson.put(Item.TAGS_KEY, new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray(item.getTags())));

    return itemJson.encode();
  }
}
