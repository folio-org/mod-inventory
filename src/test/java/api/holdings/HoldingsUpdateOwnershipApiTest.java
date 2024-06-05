package api.holdings;

import api.ApiTestSuite;
import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.HoldingsRecordUpdateOwnershipRequestBuilder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import org.apache.http.HttpStatus;
import org.folio.inventory.support.http.client.Response;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.MalformedURLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static api.ApiTestSuite.ID_FOR_FAILURE;
import static api.ApiTestSuite.createConsortiumTenant;
import static api.support.InstanceSamples.smallAngryPlanet;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static support.matchers.ResponseMatchers.hasValidationError;

@Ignore
@RunWith(JUnitParamsRunner.class)
public class HoldingsUpdateOwnershipApiTest extends ApiTests {
  private static final String INSTANCE_ID = "instanceId";

  @Before
  public void initConsortia() throws Exception {
    createConsortiumTenant();
  }

  @After
  public void clearConsortia() throws Exception {
    userTenantsClient.deleteAll();
  }

  @Test
  public void canUpdateHoldingsOwnershipToDifferentTenant() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance);
    InstanceApiClient.createInstance(consortiumOkapiClient, instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.getBody()).getJsonArray("nonUpdatedIds").size(), is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = consortiumHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord2 = consortiumHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getJson().getString(INSTANCE_ID));
  }

  @Test
  public void shouldReportErrorsWhenOnlySomeRequestedHoldingsRecordsCouldNotBeUpdated() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance);
    InstanceApiClient.createInstance(consortiumOkapiClient, instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = UUID.randomUUID();

    Assert.assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    List notFoundIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.get(0), equalTo(createHoldingsRecord2.toString()));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = consortiumHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    assertThat(instanceId.toString(), equalTo(targetTenantHoldingsRecord1.getJson().getString(INSTANCE_ID)));

    Response targetTenantHoldingsRecord2 = consortiumHoldingsStorageClient.getById(createHoldingsRecord1);
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, targetTenantHoldingsRecord2.getStatusCode());
  }

  @Test
  public void cannotUpdateHoldingsRecordsOwnershipToUnspecifiedInstance()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    JsonObject holdingsRecordUpdateOwnershipWithoutToInstanceId = new HoldingsRecordUpdateOwnershipRequestBuilder(null,
      new JsonArray(List.of(UUID.randomUUID())), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutToInstanceId);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "toInstanceId is a required field", "toInstanceId", null
    ));
  }

  @Test
  public void cannotUpdateHoldingsRecordsOwnershipToUnspecifiedTenant()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    JsonObject holdingsRecordUpdateOwnershipWithoutTenantId = new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
      new JsonArray(List.of(UUID.randomUUID())), null).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutTenantId);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "tenantId is a required field", "toInstanceId", null
    ));
  }

  @Test
  public void cannotUpdateUnspecifiedHoldingsRecordsOwnership()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    JsonObject holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds = new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
      new JsonArray(), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "Holdings record ids aren't specified", "holdingsRecordIds", null
    ));
  }

  @Test
  public void cannotUpdateHoldingsRecordOwnershipOfNonExistedInstance()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    createConsortiumTenant();

    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    UUID invalidInstanceId = UUID.randomUUID();

    InstanceApiClient.createInstance(okapiClient, instance);
    InstanceApiClient.createInstance(consortiumOkapiClient, instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);

    JsonObject holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds = new HoldingsRecordUpdateOwnershipRequestBuilder(invalidInstanceId,
      new JsonArray(List.of(createHoldingsRecord1)), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postHoldingsUpdateOwnershipResponse.getBody(), containsString("errors"));
    assertThat(postHoldingsUpdateOwnershipResponse.getBody(), containsString(invalidInstanceId.toString()));
  }

  @Test
  public void canUpdateHoldingsRecordOwnershipDueToHoldingsRecordUpdateError() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance);
    InstanceApiClient.createInstance(consortiumOkapiClient, instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(ID_FOR_FAILURE, instanceId);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    List nonUpdatedIdsIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(nonUpdatedIdsIds.size(), is(1));
    assertThat(nonUpdatedIdsIds.get(0), equalTo(ID_FOR_FAILURE.toString()));

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = consortiumHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord2 = consortiumHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(instanceId.toString(), sourceTenantHoldingsRecord2.getJson().getString(INSTANCE_ID));
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, targetTenantHoldingsRecord2.getStatusCode());
  }

  @Test
  public void canUpdateHoldingsRecordOwnershipToDifferentInstanceWithExtraRedundantFields() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance);
    InstanceApiClient.createInstance(consortiumOkapiClient, instance);

    JsonObject firstJsonHoldingsAsRequest = new HoldingRequestBuilder().forInstance(instanceId).create();
    final UUID createHoldingsRecord1 = holdingsStorageClient.create(firstJsonHoldingsAsRequest
        .put("holdingsItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID())).add(new JsonObject().put("id", UUID.randomUUID())))
        .put("bareHoldingsItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID())).add(new JsonObject().put("id", UUID.randomUUID()))))
      .getId();

    JsonObject secondJsonHoldingsAsRequest = new HoldingRequestBuilder().forInstance(instanceId).create();
    final UUID createHoldingsRecord2 = holdingsStorageClient.create(secondJsonHoldingsAsRequest
        .put("holdingsItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID())).add(new JsonObject().put("id", UUID.randomUUID())))
        .put("bareHoldingsItems", new JsonArray().add(new JsonObject().put("id", UUID.randomUUID())).add(new JsonObject().put("id", UUID.randomUUID()))))
      .getId();

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.getBody()).getJsonArray("nonUpdatedIds").size(), is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = consortiumHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord2 = consortiumHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getJson().getString(INSTANCE_ID));
  }

  private Response updateHoldingsRecordsOwnership(JsonObject holdingsRecordUpdateOwnershipRequestBody) throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var postHoldingRecordsUpdateOwnershipCompleted = okapiClient.post(
      ApiRoot.updateHoldingsRecordsOwnership(), holdingsRecordUpdateOwnershipRequestBody);
    return postHoldingRecordsUpdateOwnershipCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);
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
}
