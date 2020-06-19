package api.support.builders;

import static org.folio.inventory.domain.items.ItemsMove.ITEM_IDS;
import static org.folio.inventory.domain.items.ItemsMove.TO_HOLDINGS_RECORD_ID;

import java.util.UUID;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class ItemsMoveRequestBuilder implements Builder {

  private final UUID toHoldingsRecordId;
  private final JsonArray itemIds;

  public ItemsMoveRequestBuilder(UUID toHoldingsRecordId, JsonArray itemIds) {
    this.toHoldingsRecordId = toHoldingsRecordId;
    this.itemIds = itemIds;
  }

  public JsonObject create() {
    JsonObject itemsMoveRequest = new JsonObject();

    includeWhenPresent(itemsMoveRequest, TO_HOLDINGS_RECORD_ID, toHoldingsRecordId);
    includeWhenPresent(itemsMoveRequest, ITEM_IDS, itemIds);

    return itemsMoveRequest;
  }

  private void includeWhenPresent(JsonObject itemRequest, String property, UUID value) {
    if (value != null) {
      itemRequest.put(property, value.toString());
    }
  }

  private void includeWhenPresent(JsonObject itemRequest, String property, JsonArray value) {
    if (value != null) {
      itemRequest.put(property, value);
    }
  }
}
