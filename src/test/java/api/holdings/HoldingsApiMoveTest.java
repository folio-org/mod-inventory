package api.holdings;

import static api.ApiTestSuite.ID_FOR_FAILURE;
import static api.ApiTestSuite.createConsortiumTenant;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static support.fixtures.InstanceFixture.nod;
import static support.fixtures.InstanceFixture.smallAngryPlanet;
import static support.matchers.ResponseMatchers.hasValidationError;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.folio.inventory.support.http.client.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import support.ApiRoot;
import support.ApiTests;
import support.InstanceApiClient;
import support.builders.HoldingRequestBuilder;
import support.builders.HoldingsRecordMoveRequestBuilder;

public class HoldingsApiMoveTest extends ApiTests {

  private static final String INSTANCE_ID = "instanceId";

  @BeforeEach
  void initConsortia() {
    createConsortiumTenant();
  }

  @AfterEach
  void clearConsortia() throws Exception {
    userTenantsClient.deleteAll();
  }

  @Test
  void canMoveHoldingsToDifferentInstance() {
    UUID oldInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));
    UUID newInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, nod(newInstanceId));

    final UUID createHoldingsRecord1 = createHoldingForInstance(oldInstanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(oldInstanceId);

    assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordMoveRequestBody = new HoldingsRecordMoveRequestBuilder(newInstanceId,
      new JsonArray(Arrays.asList(createHoldingsRecord1.toString(), createHoldingsRecord2.toString()))).create();

    Response postHoldingsMoveResponse = moveHoldingsRecords(holdingsRecordMoveRequestBody);

    assertThat(postHoldingsMoveResponse.statusCode(), is(200));
    assertThat(new JsonObject(postHoldingsMoveResponse.body()).getJsonArray("nonUpdatedIds").size(), is(0));
    assertThat(postHoldingsMoveResponse.contentType(), containsString(APPLICATION_JSON));

    JsonObject holdingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();
    JsonObject holdingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();

    assertEquals(newInstanceId.toString(), holdingsRecord1.getString(INSTANCE_ID));
    assertEquals(newInstanceId.toString(), holdingsRecord2.getString(INSTANCE_ID));
  }

  @Test
  void canMoveHoldingsToDifferentInstanceAcrossTenantsWhenSharedInstanceExists() {
    UUID oldInstanceId = UUID.randomUUID();
    UUID sharedInstanceId = UUID.randomUUID(); // instance exists in central tenant

    // Create instance in source tenant
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));

    // Simulate that shared instance exists in central tenant
    createSharedInstanceInCentralTenant(sharedInstanceId); // <-- helper method to mock it

    // Create holdings linked to oldInstance
    UUID holdingsRecordId1 = createHoldingForInstance(oldInstanceId);
    UUID holdingsRecordId2 = createHoldingForInstance(oldInstanceId);

    JsonObject holdingsMoveRequestBody = new HoldingsRecordMoveRequestBuilder(
      sharedInstanceId, new JsonArray(Arrays.asList(holdingsRecordId1.toString(), holdingsRecordId2.toString()))
    ).create();

    Response postHoldingsMoveResponse = moveHoldingsRecords(holdingsMoveRequestBody);

    assertThat(postHoldingsMoveResponse.statusCode(), is(200));
    assertThat(new JsonObject(postHoldingsMoveResponse.body()).getJsonArray("nonUpdatedIds").size(), is(0));
    assertThat(postHoldingsMoveResponse.contentType(), containsString(APPLICATION_JSON));

    JsonObject updatedHoldings1 = holdingsStorageClient.getById(holdingsRecordId1).getJson();
    JsonObject updatedHoldings2 = holdingsStorageClient.getById(holdingsRecordId2).getJson();

    assertEquals(sharedInstanceId.toString(), updatedHoldings1.getString(INSTANCE_ID));
    assertEquals(sharedInstanceId.toString(), updatedHoldings2.getString(INSTANCE_ID));
  }

  @Test
  void cannotMoveHoldingsWhenSharedInstanceNotFoundAcrossTenants() {
    UUID oldInstanceId = UUID.randomUUID();
    UUID missingSharedInstanceId = UUID.randomUUID(); // will not create this shared instance

    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));

    UUID holdingsRecordId1 = createHoldingForInstance(oldInstanceId);

    JsonObject holdingsMoveRequestBody = new HoldingsRecordMoveRequestBuilder(
      missingSharedInstanceId, new JsonArray(Collections.singletonList(holdingsRecordId1.toString()))
    ).create();

    Response postHoldingsMoveResponse = moveHoldingsRecords(holdingsMoveRequestBody);

    assertThat(postHoldingsMoveResponse.statusCode(), is(422)); // Unprocessable Entity
    assertThat(postHoldingsMoveResponse.contentType(), containsString(APPLICATION_JSON));
    assertThat(postHoldingsMoveResponse.body(),
      containsString("Instance with id=" + missingSharedInstanceId + " not found"));
  }

  @Test
  void shouldReportErrorsWhenOnlySomeRequestedHoldingsRecordsCouldNotBeMoved() {
    UUID oldInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));
    UUID newInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, nod(newInstanceId));

    final UUID createHoldingsRecord1 = createHoldingForInstance(oldInstanceId);
    final UUID createHoldingsRecord2 = UUID.randomUUID();

    assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordMoveRequestBody = new HoldingsRecordMoveRequestBuilder(newInstanceId,
      new JsonArray(Arrays.asList(createHoldingsRecord1.toString(), createHoldingsRecord2.toString()))).create();

    Response postHoldingsRecordsMoveResponse = moveHoldingsRecords(holdingsRecordMoveRequestBody);

    assertThat(postHoldingsRecordsMoveResponse.statusCode(), is(200));
    assertThat(postHoldingsRecordsMoveResponse.contentType(), containsString(APPLICATION_JSON));

    var notFoundIds = postHoldingsRecordsMoveResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.getFirst(), equalTo(createHoldingsRecord2.toString()));

    JsonObject updatedHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();
    assertThat(newInstanceId.toString(), equalTo(updatedHoldingsRecord1.getString(INSTANCE_ID)));
  }

  @Test
  @SneakyThrows
  void cannotMoveHoldingsRecordsToUnspecifiedInstance() {
    JsonObject holdingsRecordMoveWithoutToInstanceId = new HoldingsRecordMoveRequestBuilder(null,
      new JsonArray(Collections.singletonList(UUID.randomUUID()))).create();

    final var postMoveHoldingsRecordCompleted = okapiClient.post(
      ApiRoot.moveHoldingsRecords(), holdingsRecordMoveWithoutToInstanceId);

    Response postMoveHoldingsRecordResponse = postMoveHoldingsRecordCompleted
      .toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(postMoveHoldingsRecordResponse.statusCode(), is(422));
    assertThat(postMoveHoldingsRecordResponse.contentType(), containsString(APPLICATION_JSON));

    assertThat(postMoveHoldingsRecordResponse, hasValidationError(
      "toInstanceId is a required field", "toInstanceId", null
    ));
  }

  @Test
  @SneakyThrows
  void cannotMoveUnspecifiedHoldingsRecords() {
    JsonObject holdingsRecordMoveWithoutHoldingsRecordIds =
      new HoldingsRecordMoveRequestBuilder(UUID.randomUUID(), new JsonArray()).create();

    final var postMoveHoldingsRecordCompleted = okapiClient.post(
      ApiRoot.moveHoldingsRecords(), holdingsRecordMoveWithoutHoldingsRecordIds);

    Response postMoveHoldingsRecordResponse = postMoveHoldingsRecordCompleted
      .toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(postMoveHoldingsRecordResponse.statusCode(), is(422));
    assertThat(postMoveHoldingsRecordResponse.contentType(), containsString(APPLICATION_JSON));

    assertThat(postMoveHoldingsRecordResponse, hasValidationError(
      "Holdings record ids aren't specified", "holdingsRecordIds", null
    ));
  }

  @Test
  void cannotMoveToNonExistedInstance() {
    UUID oldInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));
    UUID newInstanceId = UUID.randomUUID();

    final UUID createHoldingsRecord1 = createHoldingForInstance(oldInstanceId);
    final UUID createHoldingsRecord2 = UUID.randomUUID();

    assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordMoveRequestBody = new HoldingsRecordMoveRequestBuilder(newInstanceId,
      new JsonArray(Arrays.asList(createHoldingsRecord1.toString(), createHoldingsRecord2.toString()))).create();

    Response postMoveHoldingsRecordResponse = moveHoldingsRecords(holdingsRecordMoveRequestBody);

    assertThat(postMoveHoldingsRecordResponse.statusCode(), is(422));
    assertThat(postMoveHoldingsRecordResponse.contentType(), containsString(APPLICATION_JSON));

    assertThat(postMoveHoldingsRecordResponse.body(), containsString("errors"));
    assertThat(postMoveHoldingsRecordResponse.body(), containsString(newInstanceId.toString()));
  }

  @Test
  void canMoveHoldingsRecordsDueToHoldingsRecordUpdateError() {
    UUID oldInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));
    UUID newInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, nod(newInstanceId));

    final UUID createHoldingsRecord1 = createHoldingForInstance(oldInstanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(ID_FOR_FAILURE, oldInstanceId);

    assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordMoveRequestBody = new HoldingsRecordMoveRequestBuilder(newInstanceId,
      new JsonArray(Arrays.asList(createHoldingsRecord1.toString(), createHoldingsRecord2.toString()))).create();

    Response postHoldingsRecordsMoveResponse = moveHoldingsRecords(holdingsRecordMoveRequestBody);

    var nonUpdatedIdsIds = postHoldingsRecordsMoveResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(nonUpdatedIdsIds.size(), is(1));
    assertThat(nonUpdatedIdsIds.getFirst(), equalTo(ID_FOR_FAILURE.toString()));

    assertThat(postHoldingsRecordsMoveResponse.statusCode(), is(200));
    assertThat(postHoldingsRecordsMoveResponse.contentType(), containsString(APPLICATION_JSON));

    JsonObject updatedHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();
    assertThat(newInstanceId.toString(), equalTo(updatedHoldingsRecord1.getString(INSTANCE_ID)));

    JsonObject updatedHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2)
      .getJson();
    assertThat(oldInstanceId.toString(), equalTo(updatedHoldingsRecord2.getString(INSTANCE_ID)));
  }

  @Test
  void canMoveHoldingsToDifferentInstanceWithExtraRedundantFields() {
    UUID oldInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));
    UUID newInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, nod(newInstanceId));

    JsonObject firstJsonHoldingsAsRequest = new HoldingRequestBuilder().forInstance(oldInstanceId).create();
    final UUID createHoldingsRecord1 = holdingsStorageClient.create(firstJsonHoldingsAsRequest
        .put("holdingsItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID()))
          .add(new JsonObject().put("id", UUID.randomUUID())))
        .put("bareHoldingsItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID()))
          .add(new JsonObject().put("id", UUID.randomUUID()))))
      .getId();

    JsonObject secondJsonHoldingsAsRequest = new HoldingRequestBuilder().forInstance(oldInstanceId).create();
    final UUID createHoldingsRecord2 = holdingsStorageClient.create(secondJsonHoldingsAsRequest
        .put("holdingsItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID()))
          .add(new JsonObject().put("id", UUID.randomUUID())))
        .put("bareHoldingsItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID()))
          .add(new JsonObject().put("id", UUID.randomUUID()))))
      .getId();

    assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordMoveRequestBody = new HoldingsRecordMoveRequestBuilder(newInstanceId,
      new JsonArray(Arrays.asList(createHoldingsRecord1.toString(), createHoldingsRecord2.toString()))).create();

    Response postHoldingsMoveResponse = moveHoldingsRecords(holdingsRecordMoveRequestBody);

    assertThat(postHoldingsMoveResponse.statusCode(), is(200));
    assertThat(new JsonObject(postHoldingsMoveResponse.body()).getJsonArray("nonUpdatedIds").size(), is(0));
    assertThat(postHoldingsMoveResponse.contentType(), containsString(APPLICATION_JSON));

    JsonObject holdingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();
    JsonObject holdingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();

    assertEquals(newInstanceId.toString(), holdingsRecord1.getString(INSTANCE_ID));
    assertEquals(newInstanceId.toString(), holdingsRecord2.getString(INSTANCE_ID));
  }

  @SneakyThrows
  private Response moveHoldingsRecords(JsonObject holdingsRecordMoveRequestBody) {
    final var postHoldingRecordsMoveCompleted = okapiClient.post(
      ApiRoot.moveHoldingsRecords(), holdingsRecordMoveRequestBody);
    return postHoldingRecordsMoveCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private UUID createHoldingForInstance(UUID instanceId) {
    return holdingsStorageClient.create(new HoldingRequestBuilder().forInstance(instanceId))
      .getId();
  }

  private UUID createHoldingForInstance(UUID id, UUID instanceId) {
    JsonObject obj = new HoldingRequestBuilder().forInstance(instanceId).create();
    obj.put("id", id.toString());
    holdingsStorageClient.create(obj);
    return id;
  }

  private void createSharedInstanceInCentralTenant(UUID sharedInstanceId) {
    InstanceApiClient.createInstance(consortiumOkapiClient, nod(sharedInstanceId));
  }
}
