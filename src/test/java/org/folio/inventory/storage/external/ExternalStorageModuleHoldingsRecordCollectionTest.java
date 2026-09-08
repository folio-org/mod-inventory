package org.folio.inventory.storage.external;

import static api.ApiTestSuite.REQUEST_ID;
import static api.ApiTestSuite.USER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.rest.jaxrs.model.HoldingsRecord;
import org.junit.jupiter.api.Test;

class ExternalStorageModuleHoldingsRecordCollectionTest extends AbstractExternalStorageTest {

  private static final String INSTANCE_ID = UUID.randomUUID().toString();
  private static final String HOLDING_ID = UUID.randomUUID().toString();
  private static final String PERMANENT_LOCATION_ID = UUID.randomUUID().toString();

  private final ExternalStorageModuleHoldingsRecordCollection storage =
    useHttpClient(client -> new ExternalStorageModuleHoldingsRecordCollection(
      getStorageAddress(), TENANT_ID, TENANT_TOKEN, USER_ID, REQUEST_ID, client));

  @Test
  void shouldMapFromJson() {
    JsonObject holdingsRecord = new JsonObject()
      .put("id", HOLDING_ID)
      .put("instanceId", INSTANCE_ID)
      .put("permanentLocationId", PERMANENT_LOCATION_ID);

    HoldingsRecord holdingsrecord = storage.mapFromJson(holdingsRecord);
    assertNotNull(holdingsrecord);
    assertEquals(HOLDING_ID, holdingsrecord.getId());
    assertEquals(INSTANCE_ID, holdingsrecord.getInstanceId());
    assertEquals(PERMANENT_LOCATION_ID, holdingsrecord.getPermanentLocationId());
  }

  @Test
  void shouldMapFromJsonAndIgnoreUnknownProperties() {
    JsonObject holdingsRecord = new JsonObject()
      .put("holdingsItems", "testValue")
      .put("bareHoldingsItems", "testValue")
      .put("instanceId", INSTANCE_ID);

    var result = storage.mapFromJson(holdingsRecord);
    assertEquals(INSTANCE_ID, result.getInstanceId());
  }

  @Test
  void shouldRetrieveId() {
    String holdingId = UUID.randomUUID().toString();
    HoldingsRecord holdingsrecord = new HoldingsRecord()
      .withId(holdingId);
    assertEquals(holdingId, storage.getId(holdingsrecord));
  }

  @Test
  void shouldMapToRequest() {
    HoldingsRecord holdingsrecord = new HoldingsRecord()
      .withId(HOLDING_ID)
      .withInstanceId(INSTANCE_ID)
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    JsonObject jsonObject = storage.mapToRequest(holdingsrecord);
    assertNotNull(jsonObject);
    assertEquals(HOLDING_ID, jsonObject.getString("id"));
    assertEquals(INSTANCE_ID, jsonObject.getString("instanceId"));
    assertEquals(PERMANENT_LOCATION_ID, jsonObject.getString("permanentLocationId"));
  }
}
