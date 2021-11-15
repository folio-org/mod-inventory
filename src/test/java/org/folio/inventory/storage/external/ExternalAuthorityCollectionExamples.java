package org.folio.inventory.storage.external;

import static org.folio.inventory.storage.external.ExternalStorageSuite.getStorageAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import org.folio.Authority;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class ExternalAuthorityCollectionExamples {

  private final ExternalStorageModuleAuthorityCollection collection =
      ExternalStorageSuite.useVertx(
          it -> new ExternalStorageModuleAuthorityCollection(it, getStorageAddress(),
              ExternalStorageSuite.TENANT_ID, ExternalStorageSuite.TENANT_TOKEN, it.createHttpClient()));

//  @Before
//  public void before() throws InterruptedException, ExecutionException, TimeoutException {
//    CompletableFuture<Void> emptied = new CompletableFuture<Void>();
//    collection.empty(succeed(emptied), fail(emptied));
//    waitForCompletion(emptied);
//  }

  @Test
  public void shouldMapAuthorityFromJson() {
    String authorityId = UUID.randomUUID().toString();
    JsonObject authorityRecord = new JsonObject()
        .put("id", authorityId)
        .put("personalName", "test personalName")
        .put("corporateName", "test corporateName")
        .put("meetingName", "test meetingName");

    Authority authority = collection.mapFromJson(authorityRecord);
    assertNotNull(authority);
    assertEquals(authorityId, authority.getId());
    assertEquals("test personalName", authority.getPersonalName());
    assertEquals("test corporateName", authority.getCorporateName());
    assertEquals("test meetingName", authority.getMeetingName());
  }

  @Test
  public void shouldMapAuthorityToRequest() {
    String authorityId = UUID.randomUUID().toString();
    Authority authority = new Authority()
        .withId(authorityId)
        .withPersonalName("test personalName")
        .withCorporateName("test corporateName")
        .withMeetingName("test meetingName");

    JsonObject jsonObject = collection.mapToRequest(authority);
    assertNotNull(jsonObject);
    assertEquals(authorityId, jsonObject.getString("id"));
    assertEquals("test personalName", jsonObject.getString("personalName"));
    assertEquals("test corporateName", jsonObject.getString("corporateName"));
    assertEquals("test meetingName", jsonObject.getString("meetingName"));
  }

  @Test
  public void shouldRetrieveId() {
    String authorityId = UUID.randomUUID().toString();
    Authority authority = new Authority()
        .withId(authorityId);
    assertEquals(authorityId, collection.getId(authority));
  }


}
