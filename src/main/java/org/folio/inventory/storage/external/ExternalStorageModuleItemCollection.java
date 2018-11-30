package org.folio.inventory.storage.external;

import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.inventory.support.JsonHelper.getNestedProperty;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.Note;
import org.folio.inventory.support.JsonArrayHelper;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

class ExternalStorageModuleItemCollection
  extends ExternalStorageModuleCollection<Item>
  implements ItemCollection {

  ExternalStorageModuleItemCollection(Vertx vertx,
                                      String baseAddress,
                                      String tenant,
                                      String token) {

    super(vertx, String.format("%s/%s", baseAddress, "item-storage/items"),
      tenant, token, "items");
  }

  @Override
  protected Item mapFromJson(JsonObject itemFromServer) {

    List<String> copyNumberList;
    copyNumberList = JsonArrayHelper.toListOfStrings(itemFromServer.getJsonArray("copyNumbers"));

    List<JsonObject> notes = toList(
      itemFromServer.getJsonArray(Item.NOTES_KEY, new JsonArray()));

    List<Note> mappedNotes = notes.stream()
      .map(it -> new Note(it))
      .collect(Collectors.toList());

    return new Item(
      itemFromServer.getString("id"),
      itemFromServer.getString("holdingsRecordId"),
      getNestedProperty(itemFromServer, "status", "name"),
      itemFromServer.getString("materialTypeId"),
      itemFromServer.getString("permanentLoanTypeId"),
      itemFromServer.getJsonObject("metadata"))
            .setBarcode(itemFromServer.getString("barcode"))
            .setEnumeration(itemFromServer.getString("enumeration"))
            .setChronology(itemFromServer.getString("chronology"))
            .setCopyNumbers(copyNumberList)
            .setNumberOfPieces(itemFromServer.getString("numberOfPieces"))
            .setNotes(mappedNotes)
            .setPermanentLocationId(itemFromServer.getString("permanentLocationId"))
            .setTemporaryLocationId(itemFromServer.getString("temporaryLocationId"))
            .setTemporaryLoanTypeId(itemFromServer.getString("temporaryLoanTypeId"));
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

    if(item.getStatus() != null) {
      itemToSend.put("status", new JsonObject().put("name", item.getStatus()));
    }

    itemToSend.put("copyNumbers", item.getCopyNumbers());
    itemToSend.put("notes", item.getNotes());
    includeIfPresent(itemToSend, "barcode", item.getBarcode());
    includeIfPresent(itemToSend, "enumeration", item.getEnumeration());
    includeIfPresent(itemToSend, "chronology", item.getChronology());
    includeIfPresent(itemToSend, "numberOfPieces", item.getNumberOfPieces());
    includeIfPresent(itemToSend, "holdingsRecordId", item.getHoldingId());
    includeIfPresent(itemToSend, "materialTypeId", item.getMaterialTypeId());
    includeIfPresent(itemToSend, "permanentLoanTypeId", item.getPermanentLoanTypeId());
    includeIfPresent(itemToSend, "temporaryLoanTypeId", item.getTemporaryLoanTypeId());
    includeIfPresent(itemToSend, "permanentLocationId", item.getPermanentLocationId());
    includeIfPresent(itemToSend, "temporaryLocationId", item.getTemporaryLocationId());

    return itemToSend;
  }
}
