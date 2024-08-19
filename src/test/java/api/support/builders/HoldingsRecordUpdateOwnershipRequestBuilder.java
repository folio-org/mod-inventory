package api.support.builders;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

import static org.folio.inventory.resources.MoveApi.HOLDINGS_RECORD_IDS;
import static org.folio.inventory.resources.MoveApi.TO_INSTANCE_ID;
import static org.folio.inventory.support.MoveApiUtil.TARGET_TENANT_ID;

public class HoldingsRecordUpdateOwnershipRequestBuilder extends AbstractBuilder {
  private static final String TARGET_LOCATION_ID = "targetLocationId";
  private final UUID toInstanceId;
  private final JsonArray holdingsRecordsIds;
  private final String tenantId;
  private final UUID targetLocationId;

  public HoldingsRecordUpdateOwnershipRequestBuilder(UUID toInstanceId, JsonArray holdingsRecordsIds, UUID targetLocationId, String tenantId) {
    this.toInstanceId = toInstanceId;
    this.holdingsRecordsIds = holdingsRecordsIds;
    this.tenantId = tenantId;
    this.targetLocationId = targetLocationId;
  }

  public JsonObject create() {
    JsonObject holdingsRecordUpdateOwnershipRequest = new JsonObject();

    includeWhenPresent(holdingsRecordUpdateOwnershipRequest, TO_INSTANCE_ID, toInstanceId);
    includeWhenPresent(holdingsRecordUpdateOwnershipRequest, HOLDINGS_RECORD_IDS, holdingsRecordsIds);
    includeWhenPresent(holdingsRecordUpdateOwnershipRequest, TARGET_TENANT_ID, tenantId);
    includeWhenPresent(holdingsRecordUpdateOwnershipRequest, TARGET_LOCATION_ID, targetLocationId);

    return holdingsRecordUpdateOwnershipRequest;
  }
}
