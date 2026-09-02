package org.folio.inventory.storage.external;

import static api.ApiTestSuite.REQUEST_ID;
import static api.ApiTestSuite.USER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.rest.jaxrs.model.HoldingsRecordsSource;
import org.junit.jupiter.api.Test;

class ExternalStorageModuleHoldingsRecordsSourceCollectionTest extends AbstractExternalStorageTest {

  private final ExternalStorageModuleHoldingsRecordsSourceCollection storage =
    useHttpClient(client -> new ExternalStorageModuleHoldingsRecordsSourceCollection(
      getStorageAddress(), TENANT_ID, TENANT_TOKEN, USER_ID, REQUEST_ID, client));

  @Test
  void shouldMapFromJson() {
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

  @Test
  void shouldRetrieveId() {
    String sourceId = UUID.randomUUID().toString();
    HoldingsRecordsSource holdingsRecordsSource = new HoldingsRecordsSource()
      .withId(sourceId);
    assertEquals(sourceId, storage.getId(holdingsRecordsSource));
  }

  @Test
  void shouldMapToRequest() {
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
