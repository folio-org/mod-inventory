package org.folio.inventory.storage.external;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.folio.inventory.storage.external.ExternalStorageSuite.getStorageAddress;

import java.util.UUID;

import io.vertx.core.json.JsonObject;
import org.junit.Ignore;
import org.junit.Test;

import org.folio.Authority;
import org.folio.inventory.validation.exceptions.JsonMappingException;

public class ExternalStorageModuleAuthorityRecordCollectionTest {
  private static final String AUTHORITY_ID = UUID.randomUUID().toString();
  private static final String CORPORATE_NAME = UUID.randomUUID().toString();
  private static final Integer VERSION = 3;

  private final ExternalStorageModuleAuthorityRecordCollection storage =
    ExternalStorageSuite.useVertx(
      it -> new ExternalStorageModuleAuthorityRecordCollection(getStorageAddress(),
        ExternalStorageSuite.TENANT_ID, ExternalStorageSuite.TENANT_TOKEN, it.createHttpClient()));

  @Test
  public void shouldMapFromJson() {
    JsonObject authorityRecord = new JsonObject()
      .put("id", AUTHORITY_ID)
      .put("_version", VERSION)
      .put("corporateName", CORPORATE_NAME);

    Authority authority = storage.mapFromJson(authorityRecord);
    assertNotNull(authority);
    assertEquals(AUTHORITY_ID, authority.getId());
    assertEquals(VERSION, authority.getVersion());
    assertEquals(CORPORATE_NAME, authority.getCorporateName());
  }

  @Test
  public void shouldRetrieveId() {
    String authorityId = UUID.randomUUID().toString();
    Authority authority = new Authority()
      .withId(authorityId);
    assertEquals(authorityId, storage.getId(authority));
  }

  @Test
  public void shouldMapToRequest() {
    Authority authority = new Authority()
      .withId(AUTHORITY_ID)
      .withVersion(Integer.valueOf(VERSION))
      .withCorporateName(CORPORATE_NAME);

    JsonObject jsonObject = storage.mapToRequest(authority);
    assertNotNull(jsonObject);
    assertEquals(AUTHORITY_ID, jsonObject.getString("id"));
    assertEquals(VERSION.toString(), jsonObject.getString("_version"));
    assertEquals(CORPORATE_NAME, jsonObject.getString("corporateName"));
  }

}
