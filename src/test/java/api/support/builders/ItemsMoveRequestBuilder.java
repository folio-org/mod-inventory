package api.support.builders;


import java.util.UUID;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import static org.folio.inventory.resources.MoveApi.ITEM_IDS;
import static org.folio.inventory.resources.MoveApi.TO_HOLDINGS_RECORD_ID;

public class ItemsMoveRequestBuilder extends AbstractBuilder {

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
}
