package org.folio.inventory.storage.external;

import static org.folio.inventory.support.JsonArrayHelper.toList;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.folio.inventory.domain.items.CirculationNote;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.Note;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;
import org.folio.inventory.support.JsonArrayHelper;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

class ExternalStorageModuleItemCollection
  extends ExternalStorageModuleCollection<Item>
  implements ItemCollection {

  ExternalStorageModuleItemCollection(Vertx vertx,
                                      String baseAddress,
                                      String tenant,
                                      String token,
                                      HttpClient client) {

    super(vertx, String.format("%s/%s", baseAddress, "item-storage/items"),
      tenant, token, "items", client);
  }

  @Override
  protected Item mapFromJson(JsonObject itemFromServer) {

    List<String> formerIds = JsonArrayHelper
            .toListOfStrings(itemFromServer.getJsonArray(Item.FORMER_IDS_KEY));
    List<String> copyNumberList = JsonArrayHelper
            .toListOfStrings(itemFromServer.getJsonArray("copyNumbers"));
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

    return new Item(
      itemFromServer.getString("id"),
      itemFromServer.getString("holdingsRecordId"),
      new Status(itemFromServer.getJsonObject("status")),
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
            .withCopyNumbers(copyNumberList)
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
            .withTemporaryLoanTypeId(itemFromServer.getString("temporaryLoanTypeId"))
            .withAccessionNumber(itemFromServer.getString(Item.ACCESSION_NUMBER_KEY))
            .withItemIdentifier(itemFromServer.getString(Item.ITEM_IDENTIFIER_KEY))
            .withYearCaption(yearCaption)
            .withElectronicAccess(mappedElectronicAccess)
            .withStatisticalCodeIds(statisticalCodeIds)
            .withPurchaseOrderLineidentifier(itemFromServer.getString(Item.PURCHASE_ORDER_LINE_IDENTIFIER));
  }

  @Override
  protected String getId(Item record) {
    return record.id;
  }

  @Override
  protected JsonObject mapToRequest(Item item) {
    JsonObject itemToSend = new JsonObject();

    //TODO: Review if this shouldn't be defaulting here
    itemToSend.put("id", item.id != null
      ? item.id
      : UUID.randomUUID().toString());

    if(item.getStatus().getString(Status.NAME_KEY) != null) {
      itemToSend.put("status", item.getStatus());
    }

    includeIfPresent(itemToSend, Item.HRID_KEY, item.getHrid());
    itemToSend.put(Item.FORMER_IDS_KEY, item.getFormerIds());
    itemToSend.put(Item.DISCOVERY_SUPPRESS_KEY, item.getDiscoverySuppress());
    itemToSend.put("copyNumbers", item.getCopyNumbers());
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

    return itemToSend;
  }
}
