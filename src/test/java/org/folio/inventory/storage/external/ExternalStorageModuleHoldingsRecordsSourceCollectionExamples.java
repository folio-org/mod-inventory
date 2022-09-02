package org.folio.inventory.storage.external;

import io.vertx.core.json.JsonObject;
import org.folio.HoldingsRecordsSource;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.junit.Test;

import java.util.UUID;

import static org.folio.inventory.storage.external.ExternalStorageSuite.getStorageAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ExternalStorageModuleHoldingsRecordsSourceCollectionExamples {
  private final ExternalStorageModuleHoldingsRecordsSourceCollection storage =
    ExternalStorageSuite.useVertx(
      it -> new ExternalStorageModuleHoldingsRecordsSourceCollection(
              getStorageAddress(),
        ExternalStorageSuite.TENANT_ID, ExternalStorageSuite.TENANT_TOKEN, it.createHttpClient()));

  @Test
  public void shouldMapFromJson() {
    String sourceId = UUID.randomUUID().toString();
    String name = "MARC";
    JsonObject holdingsRecordsSource = new JsonObject()
      .put("id", sourceId)
      .put("name", name);

    HoldingsRecordsSource source = storage.mapFromJson(holdingsRecordsSource);
    assertNotNull(source);
    assertEquals(sourceId, source.getId());
    assertEquals(name, source.getName());
  }

  @Test(expected = JsonMappingException.class)
  public void shouldNotMapFromJsonAndThrowException() {
    JsonObject holdingsRecordsSource = new JsonObject()
      .put("testField", "testValue");

    storage.mapFromJson(holdingsRecordsSource);
  }

  @Test
  public void shouldRetrieveId() {
    String sourceId = UUID.randomUUID().toString();
    HoldingsRecordsSource holdingsRecordsSource = new HoldingsRecordsSource()
      .withId(sourceId);
    assertEquals(sourceId, storage.getId(holdingsRecordsSource));
  }

  @Test
  public void shouldMapToRequest() {
    String sourceId = UUID.randomUUID().toString();
    String name = "MARC";
    HoldingsRecordsSource holdingsRecordsSource = new HoldingsRecordsSource()
      .withId(sourceId)
      .withName(name);

    JsonObject jsonObject = storage.mapToRequest(holdingsRecordsSource);
    assertNotNull(jsonObject);
    assertEquals(sourceId, jsonObject.getString("id"));
    assertEquals(name, jsonObject.getString("name"));
  }
}
