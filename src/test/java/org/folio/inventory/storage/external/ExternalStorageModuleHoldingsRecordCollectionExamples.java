package org.folio.inventory.storage.external;

import static api.ApiTestSuite.REQUEST_ID;
import static api.ApiTestSuite.USER_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.HoldingsRecord;
import org.junit.Test;

public class ExternalStorageModuleHoldingsRecordCollectionExamples extends ExternalStorageTests {

  private static final String INSTANCE_ID = UUID.randomUUID().toString();
  private static final String HOLDING_ID = UUID.randomUUID().toString();
  private static final String PERMANENT_LOCATION_ID = UUID.randomUUID().toString();

  private final ExternalStorageModuleHoldingsRecordCollection storage =
    useHttpClient(client -> new ExternalStorageModuleHoldingsRecordCollection(
      getStorageAddress(), TENANT_ID, TENANT_TOKEN, USER_ID, REQUEST_ID, client));

  @Test
  public void shouldMapFromJson() {
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
  public void shouldMapFromJsonAndIgnoreUnknownProperties() {
    JsonObject holdingsRecord = new JsonObject()
      .put("holdingsItems", "testValue")
      .put("bareHoldingsItems", "testValue")
      .put("instanceId", INSTANCE_ID);

    var result = storage.mapFromJson(holdingsRecord);
    assertEquals(INSTANCE_ID, result.getInstanceId());
  }

  @Test
  public void shouldRetrieveId() {
    String holdingId = UUID.randomUUID().toString();
    HoldingsRecord holdingsrecord = new HoldingsRecord()
      .withId(holdingId);
    assertEquals(holdingId, storage.getId(holdingsrecord));
  }

  @Test
  public void shouldMapToRequest() {
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
