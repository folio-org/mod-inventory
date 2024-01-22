package api.holdings;

import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.HoldingsRecordMoveRequestBuilder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import org.folio.inventory.support.http.client.Response;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static api.ApiTestSuite.ID_FOR_FAILURE;
import static api.support.InstanceSamples.nod;
import static api.support.InstanceSamples.smallAngryPlanet;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static support.matchers.ResponseMatchers.hasValidationError;

@RunWith(JUnitParamsRunner.class)
public class HoldingsApiMoveExamples extends ApiTests {

  private static final String INSTANCE_ID = "instanceId";

  @Test
  public void canMoveHoldingsToDifferentInstance() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    UUID oldInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));
    UUID newInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, nod(newInstanceId));

    final UUID createHoldingsRecord1 = createHoldingForInstance(oldInstanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(oldInstanceId);

    Assert.assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordMoveRequestBody = new HoldingsRecordMoveRequestBuilder(newInstanceId,
        new JsonArray(Arrays.asList(createHoldingsRecord1.toString(), createHoldingsRecord2.toString()))).create();

    Response postHoldingsMoveResponse = moveHoldingsRecords(holdingsRecordMoveRequestBody);

    assertThat(postHoldingsMoveResponse.getStatusCode(), is(200));
    assertThat(new JsonObject(postHoldingsMoveResponse.getBody()).getJsonArray("nonUpdatedIds").size(), is(0));
    assertThat(postHoldingsMoveResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonObject holdingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();
    JsonObject holdingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();

    Assert.assertEquals(newInstanceId.toString(), holdingsRecord1.getString(INSTANCE_ID));
    Assert.assertEquals(newInstanceId.toString(), holdingsRecord2.getString(INSTANCE_ID));
  }

  @Test
  public void shouldReportErrorsWhenOnlySomeRequestedHoldingsRecordsCouldNotBeMoved() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    UUID oldInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));
    UUID newInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, nod(newInstanceId));

    final UUID createHoldingsRecord1 = createHoldingForInstance(oldInstanceId);
    final UUID createHoldingsRecord2 = UUID.randomUUID();

    Assert.assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordMoveRequestBody = new HoldingsRecordMoveRequestBuilder(newInstanceId,
      new JsonArray(Arrays.asList(createHoldingsRecord1.toString(), createHoldingsRecord2.toString()))).create();

    Response postHoldingsRecordsMoveResponse = moveHoldingsRecords(holdingsRecordMoveRequestBody);

    assertThat(postHoldingsRecordsMoveResponse.getStatusCode(), is(200));
    assertThat(postHoldingsRecordsMoveResponse.getContentType(), containsString(APPLICATION_JSON));

    List notFoundIds = postHoldingsRecordsMoveResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.get(0), equalTo(createHoldingsRecord2.toString()));

    JsonObject updatedHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();
    assertThat(newInstanceId.toString(), equalTo(updatedHoldingsRecord1.getString(INSTANCE_ID)));
  }

  @Test
  public void cannotMoveHoldingsRecordsToUnspecifiedInstance()
      throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    JsonObject holdingsRecordMoveWithoutToInstanceId = new HoldingsRecordMoveRequestBuilder(null,
      new JsonArray(Collections.singletonList(UUID.randomUUID()))).create();

    final var postMoveHoldingsRecordCompleted = okapiClient.post(
      ApiRoot.moveHoldingsRecords(), holdingsRecordMoveWithoutToInstanceId);

    Response postMoveHoldingsRecordResponse = postMoveHoldingsRecordCompleted
      .toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(postMoveHoldingsRecordResponse.getStatusCode(), is(422));
    assertThat(postMoveHoldingsRecordResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postMoveHoldingsRecordResponse, hasValidationError(
      "toInstanceId is a required field", "toInstanceId", null
    ));
  }

  @Test
  public void cannotMoveUnspecifiedHoldingsRecords()
      throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {

    JsonObject holdingsRecordMoveWithoutHoldingsRecordIds = new HoldingsRecordMoveRequestBuilder(UUID.randomUUID(), new JsonArray()).create();

    final var postMoveHoldingsRecordCompleted = okapiClient.post(
      ApiRoot.moveHoldingsRecords(), holdingsRecordMoveWithoutHoldingsRecordIds);

    Response postMoveHoldingsRecordResponse = postMoveHoldingsRecordCompleted
      .toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(postMoveHoldingsRecordResponse.getStatusCode(), is(422));
    assertThat(postMoveHoldingsRecordResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postMoveHoldingsRecordResponse, hasValidationError(
      "Holdings record ids aren't specified", "holdingsRecordIds", null
    ));
  }

  @Test
  public void cannotMoveToNonExistedInstance()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {

    UUID oldInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));
    UUID newInstanceId = UUID.randomUUID();

    final UUID createHoldingsRecord1 = createHoldingForInstance(oldInstanceId);
    final UUID createHoldingsRecord2 = UUID.randomUUID();

    Assert.assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordMoveRequestBody = new HoldingsRecordMoveRequestBuilder(newInstanceId,
      new JsonArray(Arrays.asList(createHoldingsRecord1.toString(), createHoldingsRecord2.toString()))).create();

    Response postMoveHoldingsRecordResponse = moveHoldingsRecords(holdingsRecordMoveRequestBody);

    assertThat(postMoveHoldingsRecordResponse.getStatusCode(), is(422));
    assertThat(postMoveHoldingsRecordResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postMoveHoldingsRecordResponse.getBody(), containsString("errors"));
    assertThat(postMoveHoldingsRecordResponse.getBody(), containsString(newInstanceId.toString()));
  }

  @Test
  public void canMoveHoldingsRecordsDueToHoldingsRecordUpdateError() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    UUID oldInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));
    UUID newInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, nod(newInstanceId));

    final UUID createHoldingsRecord1 = createHoldingForInstance(oldInstanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(ID_FOR_FAILURE, oldInstanceId);

    Assert.assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordMoveRequestBody = new HoldingsRecordMoveRequestBuilder(newInstanceId,
      new JsonArray(Arrays.asList(createHoldingsRecord1.toString(), createHoldingsRecord2.toString()))).create();

    Response postHoldingsRecordsMoveResponse = moveHoldingsRecords(holdingsRecordMoveRequestBody);

    List nonUpdatedIdsIds = postHoldingsRecordsMoveResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(nonUpdatedIdsIds.size(), is(1));
    assertThat(nonUpdatedIdsIds.get(0), equalTo(ID_FOR_FAILURE.toString()));

    assertThat(postHoldingsRecordsMoveResponse.getStatusCode(), is(200));
    assertThat(postHoldingsRecordsMoveResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonObject updatedHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();
    assertThat(newInstanceId.toString(), equalTo(updatedHoldingsRecord1.getString(INSTANCE_ID)));

    JsonObject updatedHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2)
      .getJson();
    assertThat(oldInstanceId.toString(), equalTo(updatedHoldingsRecord2.getString(INSTANCE_ID)));
  }

  @Test
  public void canMoveHoldingsToDifferentInstanceWithExtraRedundantFields() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    UUID oldInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, smallAngryPlanet(oldInstanceId));
    UUID newInstanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiClient, nod(newInstanceId));

    JsonObject firstJsonHoldingsAsRequest = new HoldingRequestBuilder().forInstance(oldInstanceId).create();
    final UUID createHoldingsRecord1 = holdingsStorageClient.create(firstJsonHoldingsAsRequest
        .put("holdingItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID())).add(new JsonObject().put("id", UUID.randomUUID())))
        .put("bareHoldingItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID())).add(new JsonObject().put("id", UUID.randomUUID()))))
      .getId();

    JsonObject secondJsonHoldingsAsRequest = new HoldingRequestBuilder().forInstance(oldInstanceId).create();
    final UUID createHoldingsRecord2 = holdingsStorageClient.create(secondJsonHoldingsAsRequest
        .put("holdingItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID())).add(new JsonObject().put("id", UUID.randomUUID())))
        .put("bareHoldingItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID())).add(new JsonObject().put("id", UUID.randomUUID()))))
      .getId();

    Assert.assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordMoveRequestBody = new HoldingsRecordMoveRequestBuilder(newInstanceId,
      new JsonArray(Arrays.asList(createHoldingsRecord1.toString(), createHoldingsRecord2.toString()))).create();

    Response postHoldingsMoveResponse = moveHoldingsRecords(holdingsRecordMoveRequestBody);

    assertThat(postHoldingsMoveResponse.getStatusCode(), is(200));
    assertThat(new JsonObject(postHoldingsMoveResponse.getBody()).getJsonArray("nonUpdatedIds").size(), is(0));
    assertThat(postHoldingsMoveResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonObject holdingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();
    JsonObject holdingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord1)
      .getJson();

    Assert.assertEquals(newInstanceId.toString(), holdingsRecord1.getString(INSTANCE_ID));
    Assert.assertEquals(newInstanceId.toString(), holdingsRecord2.getString(INSTANCE_ID));
  }

  private Response moveHoldingsRecords(JsonObject holdingsRecordMoveRequestBody) throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var postHoldingRecordsMoveCompleted = okapiClient.post(
      ApiRoot.moveHoldingsRecords(), holdingsRecordMoveRequestBody);
    return postHoldingRecordsMoveCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private UUID createHoldingForInstance(UUID instanceId)
      throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    return holdingsStorageClient.create(new HoldingRequestBuilder().forInstance(instanceId))
      .getId();
  }

  private UUID createHoldingForInstance(UUID id, UUID instanceId)
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    JsonObject obj = new HoldingRequestBuilder().forInstance(instanceId).create();
    obj.put("id", id.toString());
    holdingsStorageClient.create(obj);
    return id;
  }
}
