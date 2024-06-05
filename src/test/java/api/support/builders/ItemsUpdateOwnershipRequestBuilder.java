package api.support.builders;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

import static api.ApiTestSuite.TENANT_ID;
import static org.folio.inventory.resources.MoveApi.ITEM_IDS;
import static org.folio.inventory.resources.MoveApi.TO_HOLDINGS_RECORD_ID;

public class ItemsUpdateOwnershipRequestBuilder extends AbstractBuilder {
  private final UUID toHoldingsRecordId;
  private final JsonArray itemIds;
  private final String tenantId;

  public ItemsUpdateOwnershipRequestBuilder(UUID toHoldingsRecordId, JsonArray itemIds, String tenantId) {
    this.toHoldingsRecordId = toHoldingsRecordId;
    this.itemIds = itemIds;
    this.tenantId = tenantId;
  }

  public JsonObject create() {
    final var itemsUpdateOwnershipRequest = new JsonObject();

    includeWhenPresent(itemsUpdateOwnershipRequest, TO_HOLDINGS_RECORD_ID, toHoldingsRecordId);
    includeWhenPresent(itemsUpdateOwnershipRequest, ITEM_IDS, itemIds);
    includeWhenPresent(itemsUpdateOwnershipRequest, TENANT_ID, tenantId);

    return itemsUpdateOwnershipRequest;
  }
}
