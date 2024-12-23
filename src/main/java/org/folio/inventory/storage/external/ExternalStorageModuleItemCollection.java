package org.folio.inventory.storage.external;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.support.ItemUtil;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;

class ExternalStorageModuleItemCollection
  extends ExternalStorageModuleCollection<Item>
  implements ItemCollection {

  ExternalStorageModuleItemCollection(String baseAddress, String tenant,
    String token, String userId, String requestId, HttpClient client) {

    super(String.format("%s/%s", baseAddress, "item-storage/items"),
      tenant, token, userId, requestId, "items", client);
  }

  @Override
  protected Item mapFromJson(JsonObject itemFromServer) {
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
