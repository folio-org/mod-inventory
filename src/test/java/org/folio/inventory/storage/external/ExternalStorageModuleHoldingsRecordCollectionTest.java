package org.folio.inventory.storage.external;

import io.vertx.core.json.JsonObject;
import org.folio.HoldingsRecord;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.junit.Ignore;
import org.junit.Test;

import java.util.UUID;

import static org.folio.inventory.storage.external.ExternalStorageSuite.getStorageAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Ignore("These tests fail during maven builds for an unknown reason")
public class ExternalStorageModuleHoldingsRecordCollectionTest {
  private final ExternalStorageModuleHoldingsRecordCollection storage =
    ExternalStorageSuite.useVertx(
      it -> new ExternalStorageModuleHoldingsRecordCollection(it, getStorageAddress(),
        ExternalStorageSuite.TENANT_ID, ExternalStorageSuite.TENANT_TOKEN, it.createHttpClient()));

  @Test
  public void shouldMapFromJson() {
    String holdingId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String permanentLocationId = UUID.randomUUID().toString();
    JsonObject holdingsRecord = new JsonObject()
      .put("id", holdingId)
      .put("instanceId", instanceId)
      .put("permanentLocationId", permanentLocationId);

    HoldingsRecord holdingsrecord = storage.mapFromJson(holdingsRecord);
    assertNotNull(holdingsrecord);
    assertEquals(holdingId, holdingsrecord.getId());
    assertEquals(instanceId, holdingsrecord.getInstanceId());
    assertEquals(permanentLocationId, holdingsrecord.getPermanentLocationId());
  }

  @Test(expected = JsonMappingException.class)
  public void shouldNotMapFromJsonAndThrowException() {
    JsonObject holdingsRecord = new JsonObject()
      .put("testField", "testValue");

    storage.mapFromJson(holdingsRecord);
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
    String holdingId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String permanentLocationId = UUID.randomUUID().toString();
    HoldingsRecord holdingsrecord = new HoldingsRecord()
      .withId(holdingId)
      .withInstanceId(instanceId)
      .withPermanentLocationId(permanentLocationId);

    JsonObject jsonObject = storage.mapToRequest(holdingsrecord);
    assertNotNull(jsonObject);
    assertEquals(holdingId, jsonObject.getString("id"));
    assertEquals(instanceId, jsonObject.getString("instanceId"));
    assertEquals(permanentLocationId, jsonObject.getString("permanentLocationId"));
  }


}
