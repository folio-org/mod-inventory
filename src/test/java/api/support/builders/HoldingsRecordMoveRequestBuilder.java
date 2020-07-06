package api.support.builders;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

import static org.folio.inventory.resources.MoveApi.HOLDINGS_RECORD_IDS;
import static org.folio.inventory.resources.MoveApi.TO_INSTANCE_ID;

public class HoldingsRecordMoveRequestBuilder extends AbstractBuilder {

  private final UUID toInstanceId;
  private final JsonArray holdingsRecordsIds;

  public HoldingsRecordMoveRequestBuilder(UUID toInstanceId, JsonArray holdingsRecordsIds) {
    this.toInstanceId = toInstanceId;
    this.holdingsRecordsIds = holdingsRecordsIds;
  }

  public JsonObject create() {
    JsonObject holdingsRecordMoveRequest = new JsonObject();

    includeWhenPresent(holdingsRecordMoveRequest, TO_INSTANCE_ID, toInstanceId);
    includeWhenPresent(holdingsRecordMoveRequest, HOLDINGS_RECORD_IDS, holdingsRecordsIds);

    return holdingsRecordMoveRequest;
  }
}
