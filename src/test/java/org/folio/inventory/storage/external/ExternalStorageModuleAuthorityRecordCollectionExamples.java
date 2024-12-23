package org.folio.inventory.storage.external;

import static api.ApiTestSuite.REQUEST_ID;
import static api.ApiTestSuite.USER_ID;
import static org.folio.inventory.common.FutureAssistance.fail;
import static org.folio.inventory.common.FutureAssistance.getOnCompletion;
import static org.folio.inventory.common.FutureAssistance.succeed;
import static org.folio.inventory.common.FutureAssistance.waitForCompletion;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.folio.Authority;
import org.folio.inventory.common.WaitForAllFutures;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.junit.Test;

import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;

public class ExternalStorageModuleAuthorityRecordCollectionExamples extends ExternalStorageTests {
  private static final String AUTHORITY_ID = UUID.randomUUID().toString();
  private static final String CORPORATE_NAME = UUID.randomUUID().toString();
  private static final Integer VERSION = 3;

  private final ExternalStorageModuleAuthorityRecordCollection storage =
    useHttpClient(client -> new ExternalStorageModuleAuthorityRecordCollection(
      getStorageAddress(), TENANT_ID, TENANT_TOKEN, USER_ID, REQUEST_ID, client));

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

  @Test(expected = JsonMappingException.class)
  public void shouldNotMapFromJsonAndThrowException() {
    JsonObject holdingsRecord = new JsonObject()
      .put("_version", "wrongFormat");

    storage.mapFromJson(holdingsRecord);
  }

  @Test
  public void shouldMapToRequest() {
    Authority authority = new Authority()
      .withId(AUTHORITY_ID)
      .withVersion(VERSION)
      .withCorporateName(CORPORATE_NAME);

    JsonObject jsonObject = storage.mapToRequest(authority);
    assertNotNull(jsonObject);
    assertEquals(AUTHORITY_ID, jsonObject.getString("id"));
    assertEquals(VERSION.toString(), jsonObject.getString("_version"));
    assertEquals(CORPORATE_NAME, jsonObject.getString("corporateName"));
  }

  @Test
  @SneakyThrows
  public void canBeEmptied() {
    addSomeExamples(storage);

    CompletableFuture<Void> emptied = new CompletableFuture<>();
    storage.empty(succeed(emptied), fail(emptied));
    waitForCompletion(emptied);
    CompletableFuture<MultipleRecords<Authority>> findFuture = new CompletableFuture<>();

    storage.findAll(PagingParameters.defaults(),
        succeed(findFuture), fail(findFuture));

    MultipleRecords<Authority> allInstancesWrapped = getOnCompletion(findFuture);

    List<Authority> allInstances = allInstancesWrapped.records;

    assertThat(allInstances.size(), is(0));
    assertThat(allInstancesWrapped.totalRecords, is(0));
  }

  @SneakyThrows
  private static void addSomeExamples(AuthorityRecordCollection authorityCollection) {
    WaitForAllFutures<Authority> allAdded = new WaitForAllFutures<>();
    authorityCollection.add(createAuthority(), allAdded.notifySuccess(), v -> {});
    authorityCollection.add(createAuthority(), allAdded.notifySuccess(), v -> {});
    allAdded.waitForCompletion();
  }

  private static Authority createAuthority() {
    return new Authority()
        .withId(UUID.randomUUID().toString());
  }
}
