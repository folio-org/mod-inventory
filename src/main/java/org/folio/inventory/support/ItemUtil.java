package org.folio.inventory.support;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.items.CirculationNote;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.LastCheckIn;
import org.folio.inventory.domain.items.Note;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.folio.inventory.domain.converters.EntityConverters.converterForClass;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.JsonHelper.getNestedProperty;
import static org.folio.inventory.support.JsonHelper.includeIfPresent;

public final class ItemUtil {

  private ItemUtil() {
  }

  public static Item jsonToItem(JsonObject itemRequest) {
    List<String> formerIds = toListOfStrings(
      itemRequest.getJsonArray(Item.FORMER_IDS_KEY));

    List<String> statisticalCodeIds = toListOfStrings(
      itemRequest.getJsonArray(Item.STATISTICAL_CODE_IDS_KEY));

    List<String> yearCaption = toListOfStrings(
      itemRequest.getJsonArray(Item.YEAR_CAPTION_KEY));

    Status status = converterForClass(Status.class)
      .fromJson(itemRequest.getJsonObject(Item.STATUS_KEY));

    List<Note> notes = itemRequest.containsKey(Item.NOTES_KEY)
      ? JsonArrayHelper.toList(itemRequest.getJsonArray(Item.NOTES_KEY)).stream()
      .map(json -> new Note(json))
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<CirculationNote> circulationNotes = itemRequest.containsKey(Item.CIRCULATION_NOTES_KEY)
      ? JsonArrayHelper.toList(itemRequest.getJsonArray(Item.CIRCULATION_NOTES_KEY)).stream()
      .map(json -> new CirculationNote(json))
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<ElectronicAccess> electronicAccess = itemRequest.containsKey(Item.ELECTRONIC_ACCESS_KEY)
      ? JsonArrayHelper.toList(itemRequest.getJsonArray(Item.ELECTRONIC_ACCESS_KEY)).stream()
      .map(json -> new ElectronicAccess(json))
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<String> tags = itemRequest.containsKey(Item.TAGS_KEY)
      ? getTags(itemRequest) : new ArrayList<>();


    String materialTypeId = getNestedProperty(itemRequest, "materialType", "id");
    String permanentLocationId = getNestedProperty(itemRequest, "permanentLocation", "id");
    String temporaryLocationId = getNestedProperty(itemRequest, "temporaryLocation", "id");
    String permanentLoanTypeId = getNestedProperty(itemRequest, "permanentLoanType", "id");
    String temporaryLoanTypeId = getNestedProperty(itemRequest, "temporaryLoanType", "id");

    return new Item(
      itemRequest.getString("id"),
      itemRequest.getString("holdingsRecordId"),
      status,
      materialTypeId,
      permanentLoanTypeId,
      null)
      .withHrid(itemRequest.getString(Item.HRID_KEY))
      .withFormerIds(formerIds)
      .withDiscoverySuppress(itemRequest.getBoolean(Item.DISCOVERY_SUPPRESS_KEY))
      .withBarcode(itemRequest.getString("barcode"))
      .withItemLevelCallNumber(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_KEY))
      .withItemLevelCallNumberPrefix(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY))
      .withItemLevelCallNumberSuffix(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY))
      .withItemLevelCallNumberTypeId(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY))
      .withVolume(itemRequest.getString(Item.VOLUME_KEY))
      .withEnumeration(itemRequest.getString("enumeration"))
      .withChronology(itemRequest.getString("chronology"))
      .withNumberOfPieces(itemRequest.getString("numberOfPieces"))
      .withDescriptionOfPieces(itemRequest.getString(Item.DESCRIPTION_OF_PIECES_KEY))
      .withNumberOfMissingPieces(itemRequest.getString(Item.NUMBER_OF_MISSING_PIECES_KEY))
      .withMissingPieces(itemRequest.getString(Item.MISSING_PIECES_KEY))
      .withMissingPiecesDate(itemRequest.getString(Item.MISSING_PIECES_DATE_KEY))
      .withItemDamagedStatusId(itemRequest.getString(Item.ITEM_DAMAGED_STATUS_ID_KEY))
      .withItemDamagedStatusDate(itemRequest.getString(Item.ITEM_DAMAGED_STATUS_DATE_KEY))
      .withPermanentLocationId(permanentLocationId)
      .withTemporaryLocationId(temporaryLocationId)
      .withTemporaryLoanTypeId(temporaryLoanTypeId)
      .withCopyNumber(itemRequest.getString(Item.COPY_NUMBER_KEY))
      .withNotes(notes)
      .withCirculationNotes(circulationNotes)
      .withAccessionNumber(itemRequest.getString(Item.ACCESSION_NUMBER_KEY))
      .withItemIdentifier(itemRequest.getString(Item.ITEM_IDENTIFIER_KEY))
      .withYearCaption(yearCaption)
      .withElectronicAccess(electronicAccess)
      .withStatisticalCodeIds(statisticalCodeIds)
      .withPurchaseOrderLineidentifier(itemRequest.getString(Item.PURCHASE_ORDER_LINE_IDENTIFIER))
      .withLastCheckIn(LastCheckIn.from(itemRequest.getJsonObject(Item.LAST_CHECK_IN)))
      .withTags(tags);
  }

  private static List<String> getTags(JsonObject itemRequest) {
    final JsonObject tags = itemRequest.getJsonObject(Item.TAGS_KEY);
    return tags.containsKey(Item.TAG_LIST_KEY) ?
      JsonArrayHelper.toListOfStrings(tags.getJsonArray(Item.TAG_LIST_KEY)) : new ArrayList<>();
  }

  public static JsonObject mapToJson(Item item) {
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
    includeIfPresent(itemJson, "materialTypeId", item.getMaterialTypeId());
    includeIfPresent(itemJson, "permanentLoanTypeId", item.getPermanentLoanTypeId());
    includeIfPresent(itemJson, "temporaryLoanTypeId", item.getTemporaryLoanTypeId());
    includeIfPresent(itemJson, "permanentLocationId", item.getPermanentLocationId());
    includeIfPresent(itemJson, "temporaryLocationId", item.getTemporaryLocationId());
    includeIfPresent(itemJson, Item.ACCESSION_NUMBER_KEY, item.getAccessionNumber());
    includeIfPresent(itemJson, Item.ITEM_IDENTIFIER_KEY, item.getItemIdentifier());
    itemJson.put(Item.YEAR_CAPTION_KEY, item.getYearCaption());
    itemJson.put(Item.ELECTRONIC_ACCESS_KEY, item.getElectronicAccess());
    itemJson.put(Item.STATISTICAL_CODE_IDS_KEY, item.getStatisticalCodeIds());
    itemJson.put(Item.PURCHASE_ORDER_LINE_IDENTIFIER, item.getPurchaseOrderLineidentifier());
    itemJson.put(Item.TAGS_KEY, new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray(item.getTags())));

    return itemJson;
  }

  public static String mapToMappingResultRepresentation(Item item) {
    JsonObject itemJson = mapToJson(item);

    if (itemJson.getString("materialTypeId") != null) {
      itemJson.put("materialType", new JsonObject().put("id", itemJson.remove("materialTypeId")));
    }
    if (itemJson.getString("permanentLoanTypeId") != null) {
      itemJson.put("permanentLoanType", new JsonObject().put("id", itemJson.remove("permanentLoanTypeId")));
    }
    if (itemJson.getString("temporaryLoanTypeId") != null) {
      itemJson.put("temporaryLoanType", new JsonObject().put("id", itemJson.remove("temporaryLoanTypeId")));
    }
    if (itemJson.getString("permanentLocationId") != null) {
      itemJson.put("permanentLocation", new JsonObject().put("id", itemJson.remove("permanentLocationId")));
    }
    if (itemJson.getString("temporaryLocationId") != null) {
      itemJson.put("temporaryLocation", new JsonObject().put("id", itemJson.remove("temporaryLocationId")));
    }

    return itemJson.encode();
  }
}
