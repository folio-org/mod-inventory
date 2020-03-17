package org.folio.inventory.support;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.items.CirculationNote;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.LastCheckIn;
import org.folio.inventory.domain.items.Note;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.folio.inventory.domain.converters.EntityConverters.converterForClass;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.JsonHelper.getNestedProperty;

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
}
