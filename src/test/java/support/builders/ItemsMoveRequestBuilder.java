package support.builders;

import static org.folio.inventory.resources.MoveApi.ITEM_IDS;
import static org.folio.inventory.resources.MoveApi.TO_HOLDINGS_RECORD_ID;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.UUID;

public class ItemsMoveRequestBuilder extends AbstractBuilder {
  private final UUID toHoldingsRecordId;
  private final List<UUID> itemIds;

  public ItemsMoveRequestBuilder(UUID toHoldingsRecordId, List<UUID> itemIds) {
    this.toHoldingsRecordId = toHoldingsRecordId;
    this.itemIds = itemIds;
  }

  public JsonObject create() {
    final var itemsMoveRequest = new JsonObject();

    includeWhenPresent(itemsMoveRequest, TO_HOLDINGS_RECORD_ID, toHoldingsRecordId);
    includeWhenPresent(itemsMoveRequest, ITEM_IDS, new JsonArray(itemIds));

    return itemsMoveRequest;
  }
}
