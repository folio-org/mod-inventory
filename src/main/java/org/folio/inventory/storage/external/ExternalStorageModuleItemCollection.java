package org.folio.inventory.storage.external;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.support.ItemUtil;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ExternalStorageModuleItemCollection extends ExternalStorageModuleCollection<Item> implements ItemCollection {

  private static final Logger log = LogManager.getLogger(ExternalStorageModuleItemCollection.class);

  ExternalStorageModuleItemCollection(Vertx vertx, String baseAddress, String tenant, String token, HttpClient client) {
    super(String.format("%s/%s", baseAddress, "item-storage/items"), tenant, token, "items", client);
  }

  @Override
  protected Item mapFromJson(JsonObject itemFromServer) {

    //itemFromServer.put("effectiveShelvingOrder", "bdsthethagetagheth");
    log.info("item from server: " + itemFromServer.toString());
    return ItemUtil.fromStoredItemRepresentation(itemFromServer);
  }

  @Override
  protected String getId(Item record) {
    return record.id;
  }

  @Override
  protected JsonObject mapToRequest(Item item) {
    return ItemUtil.toStoredItemRepresentation(item);
  }
}
