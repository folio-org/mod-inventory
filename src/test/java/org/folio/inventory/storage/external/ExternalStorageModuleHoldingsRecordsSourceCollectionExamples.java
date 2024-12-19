package org.folio.inventory.storage.external;

import static api.ApiTestSuite.REQUEST_ID;
import static api.ApiTestSuite.USER_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import org.folio.HoldingsRecordsSource;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class ExternalStorageModuleHoldingsRecordsSourceCollectionExamples extends ExternalStorageTests {
  private final ExternalStorageModuleHoldingsRecordsSourceCollection storage =
    useHttpClient(client -> new ExternalStorageModuleHoldingsRecordsSourceCollection(
      getStorageAddress(), TENANT_ID, TENANT_TOKEN, USER_ID, REQUEST_ID, client));

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
