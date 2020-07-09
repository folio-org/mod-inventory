package api.holdings;

import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.HoldingsRecordMoveRequestBuilder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static api.ApiTestSuite.ID_FOR_FAILURE;
import static api.support.InstanceSamples.nod;
import static api.support.InstanceSamples.smallAngryPlanet;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

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

    CompletableFuture<Response> postHoldingRecordsMoveCompleted = new CompletableFuture<>();
    okapiClient.post(ApiRoot.moveHoldingsRecords(), holdingsRecordMoveRequestBody, ResponseHandler.any(postHoldingRecordsMoveCompleted));
    Response postHoldingsMoveResponse = postHoldingRecordsMoveCompleted.get(5, TimeUnit.SECONDS);

    assertThat(postHoldingsMoveResponse.getStatusCode(), is(200));
    assertThat(postHoldingsMoveResponse.getBody(), is(StringUtils.EMPTY));
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

    CompletableFuture<Response> postHoldingRecordsMoveCompleted = new CompletableFuture<>();
    okapiClient.post(ApiRoot.moveHoldingsRecords(), holdingsRecordMoveRequestBody, ResponseHandler.any(postHoldingRecordsMoveCompleted));

    Response postHoldingsRecordsMoveResponse = postHoldingRecordsMoveCompleted.get(5, TimeUnit.SECONDS);

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

    CompletableFuture<Response> postMoveHoldingsRecordCompleted = new CompletableFuture<>();

    JsonObject holdingsRecordMoveWithoutToInstanceId = new HoldingsRecordMoveRequestBuilder(null,
      new JsonArray(Collections.singletonList(UUID.randomUUID()))).create();

    okapiClient.post(ApiRoot.moveHoldingsRecords(), holdingsRecordMoveWithoutToInstanceId, ResponseHandler.any(postMoveHoldingsRecordCompleted));

    Response postMoveHoldingsRecordResponse = postMoveHoldingsRecordCompleted.get(5, TimeUnit.SECONDS);

    assertThat(postMoveHoldingsRecordResponse.getStatusCode(), is(422));
    assertThat(postMoveHoldingsRecordResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postMoveHoldingsRecordResponse.getBody(), containsString("errors"));
    assertThat(postMoveHoldingsRecordResponse.getBody(), containsString("toInstanceId"));
    assertThat(postMoveHoldingsRecordResponse.getBody(), containsString("toInstanceId is a required field"));
  }

  @Test
  public void cannotMoveUnspecifiedHoldingsRecords()
      throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Response> postMoveHoldingsRecordCompleted = new CompletableFuture<>();

    JsonObject holdingsRecordMoveWithoutHoldingsRecordIds = new HoldingsRecordMoveRequestBuilder(UUID.randomUUID(), new JsonArray()).create();

    okapiClient.post(ApiRoot.moveHoldingsRecords(), holdingsRecordMoveWithoutHoldingsRecordIds, ResponseHandler.any(postMoveHoldingsRecordCompleted));

    Response postMoveHoldingsRecordResponse = postMoveHoldingsRecordCompleted.get(5, TimeUnit.SECONDS);

    assertThat(postMoveHoldingsRecordResponse.getStatusCode(), is(422));
    assertThat(postMoveHoldingsRecordResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postMoveHoldingsRecordResponse.getBody(), containsString("errors"));
    assertThat(postMoveHoldingsRecordResponse.getBody(), containsString("holdingsRecordIds"));
    assertThat(postMoveHoldingsRecordResponse.getBody(), containsString("Holdings record ids aren't specified"));
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

    CompletableFuture<Response> postHoldingRecordsMoveCompleted = new CompletableFuture<>();
    okapiClient.post(ApiRoot.moveHoldingsRecords(), holdingsRecordMoveRequestBody, ResponseHandler.any(postHoldingRecordsMoveCompleted));


    Response postMoveHoldingsRecordResponse = postHoldingRecordsMoveCompleted.get(5, TimeUnit.SECONDS);

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

    CompletableFuture<Response> postHoldingRecordsMoveCompleted = new CompletableFuture<>();
    okapiClient.post(ApiRoot.moveHoldingsRecords(), holdingsRecordMoveRequestBody, ResponseHandler.any(postHoldingRecordsMoveCompleted));
    Response postHoldingsRecordsMoveResponse = postHoldingRecordsMoveCompleted.get(5, TimeUnit.SECONDS);

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
