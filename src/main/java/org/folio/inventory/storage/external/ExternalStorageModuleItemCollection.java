package org.folio.inventory.storage.external;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import org.folio.inventory.domain.Item;
import org.folio.inventory.domain.ItemCollection;

import java.util.UUID;
import org.folio.inventory.support.JsonArrayHelper;

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

    List<String> pieceIdentifierList;
    pieceIdentifierList = JsonArrayHelper.toListOfStrings(itemFromServer.getJsonArray("pieceIdentifiers"));

    List<String> notesList;
    notesList = JsonArrayHelper.toListOfStrings(itemFromServer.getJsonArray("notes"));

    return new Item(
      itemFromServer.getString("id"),
      itemFromServer.getString("title"),
      itemFromServer.getString("barcode"),
      itemFromServer.getString("enumeration"),
      itemFromServer.getString("chronology"),
      pieceIdentifierList,
      itemFromServer.getString("numberOfPieces"),
      itemFromServer.getString("instanceId"),
      notesList,
      itemFromServer.getJsonObject("status").getString("name"),
      itemFromServer.getString("materialTypeId"),
      itemFromServer.getString("permanentLocationId"),
      itemFromServer.getString("temporaryLocationId"),
      itemFromServer.getString("permanentLoanTypeId"),
      itemFromServer.getString("temporaryLoanTypeId"));
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

    itemToSend.put("title", item.title);
    itemToSend.put("status", new JsonObject().put("name", item.status));
    itemToSend.put("pieceIdentifiers", item.pieceIdentifiers);
    itemToSend.put("notes", item.notes);
    includeIfPresent(itemToSend, "barcode", item.barcode);
    includeIfPresent(itemToSend, "enumeration", item.enumeration);
    includeIfPresent(itemToSend, "chronology", item.chronology);
    includeIfPresent(itemToSend, "numberOfPieces", item.numberOfPieces);
    includeIfPresent(itemToSend, "instanceId", item.instanceId);
    includeIfPresent(itemToSend, "materialTypeId", item.materialTypeId);
    includeIfPresent(itemToSend, "permanentLoanTypeId", item.permanentLoanTypeId);
    includeIfPresent(itemToSend, "temporaryLoanTypeId", item.temporaryLoanTypeId);
    includeIfPresent(itemToSend, "permanentLocationId", item.permanentLocationId);
    includeIfPresent(itemToSend, "temporaryLocationId", item.temporaryLocationId);

    return itemToSend;
  }
}
