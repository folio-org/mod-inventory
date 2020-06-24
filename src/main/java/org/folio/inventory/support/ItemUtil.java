package org.folio.inventory.support;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.items.CirculationNote;
import org.folio.inventory.domain.items.EffectiveCallNumberComponents;
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
import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.JsonHelper.getNestedProperty;
import static org.folio.inventory.support.JsonHelper.includeIfPresent;

public final class ItemUtil {

  private static final String MATERIAL_TYPE_ID_KEY = "materialTypeId";
  private static final String PERMANENT_LOAN_TYPE_ID_KEY = "permanentLoanTypeId";
  private static final String TEMPORARY_LOAN_TYPE_ID_KEY = "temporaryLoanTypeId";
  private static final String PERMANENT_LOCATION_ID_KEY = "permanentLocationId";
  private static final String TEMPORARY_LOCATION_ID_KEY = "temporaryLocationId";

  private ItemUtil() {
  }

  public static Item jsonToServerItem(JsonObject itemFromServer) {
    List<String> formerIds = JsonArrayHelper
      .toListOfStrings(itemFromServer.getJsonArray(Item.FORMER_IDS_KEY));
    List<String> statisticalCodeIds = JsonArrayHelper
      .toListOfStrings(itemFromServer.getJsonArray(Item.STATISTICAL_CODE_IDS_KEY));
    List<String> yearCaption = JsonArrayHelper
      .toListOfStrings(itemFromServer.getJsonArray(Item.YEAR_CAPTION_KEY));

    List<JsonObject> notes = toList(
      itemFromServer.getJsonArray(Item.NOTES_KEY, new JsonArray()));

    List<Note> mappedNotes = notes.stream()
      .map(it -> new Note(it))
      .collect(Collectors.toList());

    List<JsonObject> circulationNotes = toList(
      itemFromServer.getJsonArray(Item.CIRCULATION_NOTES_KEY, new JsonArray()));

    List<CirculationNote> mappedCirculationNotes = circulationNotes.stream()
      .map(it -> new CirculationNote(it))
      .collect(Collectors.toList());

    List<JsonObject> electronicAccess = toList(
      itemFromServer.getJsonArray(Item.ELECTRONIC_ACCESS_KEY, new JsonArray()));

    List<ElectronicAccess> mappedElectronicAccess = electronicAccess.stream()
      .map(it -> new ElectronicAccess(it))
      .collect(Collectors.toList());

    List<String> tags = itemFromServer.containsKey(Item.TAGS_KEY)
      ? JsonArrayHelper.toListOfStrings(
      itemFromServer.getJsonObject(Item.TAGS_KEY).getJsonArray(Item.TAG_LIST_KEY))
      : new ArrayList<>();

    return new Item(
      itemFromServer.getString("id"),
      itemFromServer.getString("holdingsRecordId"),
      converterForClass(Status.class).fromJson(itemFromServer.getJsonObject("status")),
      itemFromServer.getString("materialTypeId"),
      itemFromServer.getString("permanentLoanTypeId"),
      itemFromServer.getJsonObject("metadata"))
      .withHrid(itemFromServer.getString(Item.HRID_KEY))
      .withFormerIds(formerIds)
      .withDiscoverySuppress(itemFromServer.getBoolean(Item.DISCOVERY_SUPPRESS_KEY))
      .withBarcode(itemFromServer.getString("barcode"))
      .withItemLevelCallNumber(itemFromServer.getString(Item.ITEM_LEVEL_CALL_NUMBER_KEY))
      .withItemLevelCallNumberPrefix(itemFromServer.getString(Item.ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY))
      .withItemLevelCallNumberSuffix(itemFromServer.getString(Item.ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY))
      .withItemLevelCallNumberTypeId(itemFromServer.getString(Item.ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY))
      .withVolume(itemFromServer.getString(Item.VOLUME_KEY))
      .withEnumeration(itemFromServer.getString("enumeration"))
      .withChronology(itemFromServer.getString("chronology"))
      .withCopyNumber(itemFromServer.getString("copyNumber"))
      .withNumberOfPieces(itemFromServer.getString("numberOfPieces"))
      .withDescriptionOfPieces(itemFromServer.getString(Item.DESCRIPTION_OF_PIECES_KEY))
      .withNumberOfMissingPieces(itemFromServer.getString(Item.NUMBER_OF_MISSING_PIECES_KEY))
      .withMissingPieces(itemFromServer.getString(Item.MISSING_PIECES_KEY))
      .withMissingPiecesDate(itemFromServer.getString(Item.MISSING_PIECES_DATE_KEY))
      .withItemDamagedStatusId(itemFromServer.getString(Item.ITEM_DAMAGED_STATUS_ID_KEY))
      .withItemDamagedStatusDate(itemFromServer.getString(Item.ITEM_DAMAGED_STATUS_DATE_KEY))
      .withNotes(mappedNotes)
      .withCirculationNotes(mappedCirculationNotes)
      .withPermanentLocationId(itemFromServer.getString("permanentLocationId"))
      .withTemporaryLocationId(itemFromServer.getString("temporaryLocationId"))
      .withEffectiveLocationId(itemFromServer.getString("effectiveLocationId"))
      .withTemporaryLoanTypeId(itemFromServer.getString("temporaryLoanTypeId"))
      .withAccessionNumber(itemFromServer.getString(Item.ACCESSION_NUMBER_KEY))
      .withItemIdentifier(itemFromServer.getString(Item.ITEM_IDENTIFIER_KEY))
      .withYearCaption(yearCaption)
      .withElectronicAccess(mappedElectronicAccess)
      .withStatisticalCodeIds(statisticalCodeIds)
      .withPurchaseOrderLineidentifier(itemFromServer.getString(Item.PURCHASE_ORDER_LINE_IDENTIFIER))
      .withTags(tags)
      .withLastCheckIn(LastCheckIn.from(itemFromServer.getJsonObject("lastCheckIn")))
      .withEffectiveCallNumberComponents(
        EffectiveCallNumberComponents.from(itemFromServer.getJsonObject("effectiveCallNumberComponents")));

  }

  public static final JsonObject serverItemToJson(Item item) {
    JsonObject itemToSend = new JsonObject();

    //TODO: Review if this shouldn't be defaulting here
    itemToSend.put("id", item.id != null
      ? item.id
      : UUID.randomUUID().toString());

    itemToSend.put("status", converterForClass(Status.class).toJson(item.getStatus()));

    if(item.getLastCheckIn() != null) {
      itemToSend.put(Item.LAST_CHECK_IN, item.getLastCheckIn().toJson());
    }

    includeIfPresent(itemToSend, Item.HRID_KEY, item.getHrid());
    itemToSend.put(Item.FORMER_IDS_KEY, item.getFormerIds());
    itemToSend.put(Item.DISCOVERY_SUPPRESS_KEY, item.getDiscoverySuppress());
    includeIfPresent(itemToSend, "copyNumber", item.getCopyNumber());
    itemToSend.put("notes", item.getNotes());
    itemToSend.put(Item.CIRCULATION_NOTES_KEY, item.getCirculationNotes());
    includeIfPresent(itemToSend, "barcode", item.getBarcode());
    includeIfPresent(itemToSend, Item.ITEM_LEVEL_CALL_NUMBER_KEY, item.getItemLevelCallNumber());
    includeIfPresent(itemToSend, Item.ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY, item.getItemLevelCallNumberPrefix());
    includeIfPresent(itemToSend, Item.ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY, item.getItemLevelCallNumberSuffix());
    includeIfPresent(itemToSend, Item.ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY, item.getItemLevelCallNumberTypeId());
    includeIfPresent(itemToSend, Item.VOLUME_KEY, item.getVolume());
    includeIfPresent(itemToSend, "enumeration", item.getEnumeration());
    includeIfPresent(itemToSend, "chronology", item.getChronology());
    includeIfPresent(itemToSend, "numberOfPieces", item.getNumberOfPieces());
    includeIfPresent(itemToSend, Item.DESCRIPTION_OF_PIECES_KEY, item.getDescriptionOfPieces());
    includeIfPresent(itemToSend, Item.NUMBER_OF_MISSING_PIECES_KEY, item.getNumberOfMissingPieces());
    includeIfPresent(itemToSend, Item.MISSING_PIECES_KEY, item.getMissingPieces());
    includeIfPresent(itemToSend, Item.MISSING_PIECES_DATE_KEY, item.getMissingPiecesDate());
    includeIfPresent(itemToSend, Item.ITEM_DAMAGED_STATUS_ID_KEY, item.getItemDamagedStatusId());
    includeIfPresent(itemToSend, Item.ITEM_DAMAGED_STATUS_DATE_KEY, item.getItemDamagedStatusDate());
    includeIfPresent(itemToSend, "holdingsRecordId", item.getHoldingId());
    includeIfPresent(itemToSend, "materialTypeId", item.getMaterialTypeId());
    includeIfPresent(itemToSend, "permanentLoanTypeId", item.getPermanentLoanTypeId());
    includeIfPresent(itemToSend, "temporaryLoanTypeId", item.getTemporaryLoanTypeId());
    includeIfPresent(itemToSend, "permanentLocationId", item.getPermanentLocationId());
    includeIfPresent(itemToSend, "temporaryLocationId", item.getTemporaryLocationId());
    includeIfPresent(itemToSend, Item.ACCESSION_NUMBER_KEY, item.getAccessionNumber());
    includeIfPresent(itemToSend, Item.ITEM_IDENTIFIER_KEY, item.getItemIdentifier());
    itemToSend.put(Item.YEAR_CAPTION_KEY, item.getYearCaption());
    itemToSend.put(Item.ELECTRONIC_ACCESS_KEY, item.getElectronicAccess());
    itemToSend.put(Item.STATISTICAL_CODE_IDS_KEY, item.getStatisticalCodeIds());
    itemToSend.put(Item.PURCHASE_ORDER_LINE_IDENTIFIER, item.getPurchaseOrderLineidentifier());
    itemToSend.put(Item.TAGS_KEY, new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray(item.getTags())));

    return itemToSend;
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
    includeIfPresent(itemJson, MATERIAL_TYPE_ID_KEY, item.getMaterialTypeId());
    includeIfPresent(itemJson, PERMANENT_LOAN_TYPE_ID_KEY, item.getPermanentLoanTypeId());
    includeIfPresent(itemJson, TEMPORARY_LOAN_TYPE_ID_KEY, item.getTemporaryLoanTypeId());
    includeIfPresent(itemJson, PERMANENT_LOCATION_ID_KEY, item.getPermanentLocationId());
    includeIfPresent(itemJson, TEMPORARY_LOCATION_ID_KEY, item.getTemporaryLocationId());
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

    if (itemJson.getString(MATERIAL_TYPE_ID_KEY) != null) {
      itemJson.put("materialType", new JsonObject().put("id", itemJson.remove(MATERIAL_TYPE_ID_KEY)));
    }
    if (itemJson.getString(PERMANENT_LOAN_TYPE_ID_KEY) != null) {
      itemJson.put("permanentLoanType", new JsonObject().put("id", itemJson.remove(PERMANENT_LOAN_TYPE_ID_KEY)));
    }
    if (itemJson.getString(TEMPORARY_LOAN_TYPE_ID_KEY) != null) {
      itemJson.put("temporaryLoanType", new JsonObject().put("id", itemJson.remove(TEMPORARY_LOAN_TYPE_ID_KEY)));
    }
    if (itemJson.getString(PERMANENT_LOCATION_ID_KEY) != null) {
      itemJson.put("permanentLocation", new JsonObject().put("id", itemJson.remove(PERMANENT_LOCATION_ID_KEY)));
    }
    if (itemJson.getString(TEMPORARY_LOCATION_ID_KEY) != null) {
      itemJson.put("temporaryLocation", new JsonObject().put("id", itemJson.remove(TEMPORARY_LOCATION_ID_KEY)));
    }

    return itemJson.encode();
  }
}
