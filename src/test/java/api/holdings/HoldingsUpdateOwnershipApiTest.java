package api.holdings;

import api.ApiTestSuite;
import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.HoldingsRecordUpdateOwnershipRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import org.apache.http.HttpStatus;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.support.http.client.Response;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import support.fakes.EndpointFailureDescriptor;

import java.net.MalformedURLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static api.ApiTestSuite.createConsortiumTenant;
import static api.support.InstanceSamples.smallAngryPlanet;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;
import static org.folio.inventory.support.ItemUtil.HOLDINGS_RECORD_ID;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static support.matchers.ResponseMatchers.hasValidationError;

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

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.getBody()).getJsonArray("nonUpdatedIds").size(), is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord2 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getJson().getString(INSTANCE_ID));
  }

  @Test
  public void canUpdateHoldingsOwnershipWithRelatedItemsToDifferentTenant() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var secondItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord2)
        .withBarcode("645398607546")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.getBody()).getJsonArray("nonUpdatedIds").size(), is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    // Verify Holdings ownership updated
    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord2 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getJson().getString(INSTANCE_ID));

    // Verify related Items ownership updated
    Response sourceTenantItem1 = itemsClient.getById(firstItem.getId());
    Response targetTenantItem1 = collegeItemsClient.getById(firstItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceTenantItem1.getStatusCode()));
    assertThat(targetTenantItem1.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord1.toString()));

    Response sourceTenantItem2 = itemsClient.getById(secondItem.getId());
    Response targetTenantItem2 = collegeItemsClient.getById(secondItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceTenantItem2.getStatusCode()));
    assertThat(targetTenantItem2.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));
  }

  @Test
  public void canUpdateHoldingsOwnershipIfErrorUpdatingRelatedItemsToDifferentTenant() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var secondItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord2)
        .withBarcode("645398607546")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Server error");
    collegeItemsClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.POST.name()));

    holdingsStorageClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.DELETE.name()));

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));

    List nonUpdatedIdsIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(nonUpdatedIdsIds.size(), is(2));
    assertThat(nonUpdatedIdsIds.get(0), equalTo(createHoldingsRecord1.toString()));
    assertThat(nonUpdatedIdsIds.get(1), equalTo(createHoldingsRecord2.toString()));

    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    // Verify Holdings ownership updated
    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(instanceId.toString(), sourceTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord2 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(instanceId.toString(), sourceTenantHoldingsRecord2.getJson().getString(INSTANCE_ID));
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getJson().getString(INSTANCE_ID));

    // Verify related Items ownership updated
    Response sourceTenantItem1 = itemsClient.getById(firstItem.getId());
    Response targetTenantItem1 = collegeItemsClient.getById(firstItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(targetTenantItem1.getStatusCode()));
    assertThat(sourceTenantItem1.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord1.toString()));

    Response sourceTenantItem2 = itemsClient.getById(secondItem.getId());
    Response targetTenantItem2 = collegeItemsClient.getById(secondItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(targetTenantItem2.getStatusCode()));
    assertThat(sourceTenantItem2.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));

    collegeItemsClient.disableFailureEmulation();
    holdingsStorageClient.disableFailureEmulation();
  }

  @Test
  public void shouldReportErrorsWhenOnlySomeRequestedHoldingsRecordsCouldNotBeUpdated() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = UUID.randomUUID();

    Assert.assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    List notFoundIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.get(0), equalTo(createHoldingsRecord2.toString()));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    assertThat(instanceId.toString(), equalTo(targetTenantHoldingsRecord1.getJson().getString(INSTANCE_ID)));

    Response targetTenantHoldingsRecord2 = collegeHoldingsStorageClient.getById(createHoldingsRecord2);
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, targetTenantHoldingsRecord2.getStatusCode());
  }

  @Test
  public void cannotUpdateHoldingsRecordsOwnershipToUnspecifiedInstance()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    JsonObject holdingsRecordUpdateOwnershipWithoutToInstanceId = new HoldingsRecordUpdateOwnershipRequestBuilder(null,
      new JsonArray(List.of(UUID.randomUUID())), ApiTestSuite.COLLEGE_TENANT_ID).create();

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
      "targetTenantId is a required field", "targetTenantId", null
    ));
  }

  @Test
  public void cannotUpdateHoldingsRecordOwnershipToSameTenant()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
      new JsonArray(List.of(UUID.randomUUID().toString())), ApiTestSuite.TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "targetTenantId field cannot be equal to source tenant id", "targetTenantId", ApiTestSuite.TENANT_ID
    ));
  }

  @Test
  public void cannotUpdateUnspecifiedHoldingsRecordsOwnership()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    JsonObject holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds = new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
      new JsonArray(), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "holdingsRecordIds is a required field", "holdingsRecordIds", null
    ));
  }

  @Test
  public void cannotUpdateHoldingsRecordOwnershipIfTenantNotInConsortium()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    userTenantsClient.deleteAll();

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
      new JsonArray(List.of(UUID.randomUUID().toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(400));

    assertThat(postHoldingsUpdateOwnershipResponse.getBody(), containsString("tenant is not in consortia"));
    createConsortiumTenant();
  }

  @Test
  public void cannotUpdateHoldingsRecordOwnershipOfNonExistedInstance()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    UUID invalidInstanceId = UUID.randomUUID();

    InstanceApiClient.createInstance(okapiClient, instance);
    InstanceApiClient.createInstance(consortiumOkapiClient, instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);

    JsonObject holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds = new HoldingsRecordUpdateOwnershipRequestBuilder(invalidInstanceId,
      new JsonArray(List.of(createHoldingsRecord1)), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(404));

    assertThat(postHoldingsUpdateOwnershipResponse.getBody(), containsString("not found"));
    assertThat(postHoldingsUpdateOwnershipResponse.getBody(), containsString(invalidInstanceId.toString()));
  }

  @Test
  public void cannotUpdateHoldingsRecordOwnershipOfNonSharedInstance()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(400));

    assertThat(postHoldingsUpdateOwnershipResponse.getBody(), containsString(String.format("Instance with id: %s is not shared", instanceId)));
  }

  @Test
  public void cannotUpdateHoldingsRecordOwnershipDueToHoldingsRecordCreateError() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId);

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Server error");
    collegeHoldingsStorageClient.emulateFailure(
      new EndpointFailureDescriptor()
      .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
      .setStatusCode(500)
      .setContentType("application/json")
      .setBody(expectedErrorResponse.toString())
      .setMethod(HttpMethod.POST.name()));

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    List nonUpdatedIdsIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(nonUpdatedIdsIds.size(), is(2));
    assertThat(nonUpdatedIdsIds.get(0), equalTo(createHoldingsRecord1.toString()));
    assertThat(nonUpdatedIdsIds.get(1), equalTo(createHoldingsRecord2.toString()));

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(instanceId.toString(), sourceTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, targetTenantHoldingsRecord1.getStatusCode());

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);
    Response targetTenantHoldingsRecord2 = collegeHoldingsStorageClient.getById(createHoldingsRecord2);

    Assert.assertEquals(instanceId.toString(), sourceTenantHoldingsRecord2.getJson().getString(INSTANCE_ID));
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, targetTenantHoldingsRecord2.getStatusCode());

    collegeHoldingsStorageClient.disableFailureEmulation();
  }

  @Test
  public void cannotUpdateHoldingsRecordOwnershipDueToHoldingsRecordDeleteError() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId);

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Server error");
    collegeHoldingsStorageClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.DELETE.name()));

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    List nonUpdatedIdsIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("nonUpdatedIds")
      .getList();

    assertThat(nonUpdatedIdsIds.size(), is(2));
    assertThat(nonUpdatedIdsIds.get(0), equalTo(createHoldingsRecord1.toString()));
    assertThat(nonUpdatedIdsIds.get(1), equalTo(createHoldingsRecord2.toString()));

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(instanceId.toString(), sourceTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);
    Response targetTenantHoldingsRecord2 = collegeHoldingsStorageClient.getById(createHoldingsRecord2);

    Assert.assertEquals(instanceId.toString(), sourceTenantHoldingsRecord2.getJson().getString(INSTANCE_ID));
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getJson().getString(INSTANCE_ID));

    collegeHoldingsStorageClient.disableFailureEmulation();
  }

  @Test
  public void canUpdateHoldingsRecordOwnershipToDifferentInstanceWithExtraRedundantFields() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

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
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.getBody()).getJsonArray("nonUpdatedIds").size(), is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord2 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
    Assert.assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getJson().getString(INSTANCE_ID));
  }

  private Response updateHoldingsRecordsOwnership(JsonObject holdingsRecordUpdateOwnershipRequestBody) throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var postHoldingRecordsUpdateOwnershipCompleted = okapiClient.post(
      ApiRoot.updateHoldingsRecordsOwnership(), holdingsRecordUpdateOwnershipRequestBody);
    return postHoldingRecordsUpdateOwnershipCompleted.toCompletableFuture().get(50, TimeUnit.SECONDS);
  }

  private UUID createHoldingForInstance(UUID instanceId) {
    return holdingsStorageClient.create(new HoldingRequestBuilder().forInstance(instanceId))
      .getId();
  }
}
