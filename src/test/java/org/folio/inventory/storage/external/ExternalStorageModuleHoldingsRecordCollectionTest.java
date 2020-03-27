package org.folio.inventory.storage.external;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import org.folio.HoldingsRecord;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class ExternalStorageModuleHoldingsRecordCollectionTest {

  private ExternalStorageModuleHoldingsRecordCollection storage;

  @Before
  public void before() {
    storage = new ExternalStorageModuleHoldingsRecordCollection(null, null, null, null, null);
  }

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
    assertNotNull(holdingsRecord);
    assertEquals(holdingId, holdingsRecord.getString("id"));
    assertEquals(instanceId, holdingsRecord.getString("instanceId"));
    assertEquals(permanentLocationId, holdingsRecord.getString("permanentLocationId"));
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
