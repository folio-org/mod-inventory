package api.support.builders;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

import static api.ApiTestSuite.TENANT_ID;
import static org.folio.inventory.resources.MoveApi.HOLDINGS_RECORD_IDS;
import static org.folio.inventory.resources.MoveApi.TO_INSTANCE_ID;

public class HoldingsRecordUpdateOwnershipRequestBuilder extends AbstractBuilder {

  private final UUID toInstanceId;
  private final JsonArray holdingsRecordsIds;
  private final String tenantId;

  public HoldingsRecordUpdateOwnershipRequestBuilder(UUID toInstanceId, JsonArray holdingsRecordsIds, String tenantId) {
    this.toInstanceId = toInstanceId;
    this.holdingsRecordsIds = holdingsRecordsIds;
    this.tenantId = tenantId;
  }

  public JsonObject create() {
    JsonObject holdingsRecordUpdateOwnershipRequest = new JsonObject();

    includeWhenPresent(holdingsRecordUpdateOwnershipRequest, TO_INSTANCE_ID, toInstanceId);
    includeWhenPresent(holdingsRecordUpdateOwnershipRequest, HOLDINGS_RECORD_IDS, holdingsRecordsIds);
    includeWhenPresent(holdingsRecordUpdateOwnershipRequest, TENANT_ID, tenantId);

    return holdingsRecordUpdateOwnershipRequest;
  }
}
