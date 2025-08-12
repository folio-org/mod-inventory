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
import org.junit.Before;
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
import static api.ApiTestSuite.getMainLibraryLocation;
import static api.BoundWithTests.makeObjectBoundWithPart;
import static api.support.InstanceSamples.smallAngryPlanet;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;
import static org.folio.inventory.resources.UpdateOwnershipApi.HOLDINGS_RECORD_NOT_FOUND;
import static org.folio.inventory.resources.UpdateOwnershipApi.HOLDINGS_RECORD_NOT_LINKED_TO_SHARED_INSTANCE;
import static org.folio.inventory.resources.UpdateOwnershipApi.HOLDING_BOUND_WITH_PARTS_ERROR;
import static org.folio.inventory.support.ItemUtil.HOLDINGS_RECORD_ID;
import static org.folio.inventory.support.ItemUtil.PERMANENT_LOCATION_ID_KEY;
import static org.folio.inventory.support.ItemUtil.TEMPORARY_LOCATION_ID_KEY;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static support.matchers.ResponseMatchers.hasValidationError;

@RunWith(JUnitParamsRunner.class)
public class HoldingsUpdateOwnershipApiTest extends ApiTests {
  private static final String INSTANCE_ID = "instanceId";
  private static final String ID = "id";

  @Before
  public void initConsortia() throws Exception {
    createConsortiumTenant();

    holdingsStorageClient.deleteAll();
    collegeHoldingsStorageClient.deleteAll();

    itemsClient.deleteAll();
    collegeItemsClient.deleteAll();
    boundWithPartsStorageClient.deleteAll();

    sourceRecordStorageClient.deleteAll();
    collegeSourceRecordStorageClient.deleteAll();
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
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(HttpStatus.SC_OK));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.getBody()).getJsonArray("notUpdatedEntities").size(), is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(2, targetTenantHoldings.size());

    var targetTenantHoldingIds = targetTenantHoldings.stream().map(object -> object.getString(ID))
      .toList();

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));
    assertEquals(getMainLibraryLocation(), targetTenantHoldingsRecord1.getString(PERMANENT_LOCATION_ID_KEY));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);
    JsonObject targetTenantHoldingsRecord2 = targetTenantHoldings.get(1);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getString(INSTANCE_ID));
    assertEquals(getMainLibraryLocation(), targetTenantHoldingsRecord2.getString(PERMANENT_LOCATION_ID_KEY));
    assertNull(targetTenantHoldingsRecord2.getString("hrid"));
    assertTrue(targetTenantHoldingIds.contains(createHoldingsRecord1.toString()));
    assertTrue(targetTenantHoldingIds.contains(createHoldingsRecord2.toString()));
  }

  @Test
  public void canUpdateHoldingsOwnershipWithRelatedItemsToDifferentTenant() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    String itemHrId = "it0000001";
    String locationId = UUID.randomUUID().toString();
    JsonObject location = new JsonObject().put("id", locationId).put("name", "location");

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value())
        .withTemporaryLocation(location)
        .withPermanentLocation(location));

    final var secondItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord2)
        .withHrid(itemHrId)
        .withBarcode("645398607546")
        .withStatus(ItemStatusName.AVAILABLE.value())
        .withTemporaryLocation(location)
        .withPermanentLocation(location));

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(HttpStatus.SC_OK));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.getBody()).getJsonArray("notUpdatedEntities").size(), is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    // Verify Holdings ownership updated
    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(2, targetTenantHoldings.size());

    var targetTenantHoldingIds = targetTenantHoldings.stream().map(object -> object.getString(ID))
      .toList();

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.get(1);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));
    assertTrue(targetTenantHoldingIds.contains(createHoldingsRecord1.toString()));
    assertEquals(getMainLibraryLocation(), targetTenantHoldingsRecord1.getString(PERMANENT_LOCATION_ID_KEY));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);
    JsonObject targetTenantHoldingsRecord2 = targetTenantHoldings.get(1);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getString(INSTANCE_ID));
    assertTrue(targetTenantHoldingIds.contains(createHoldingsRecord2.toString()));
    assertEquals(getMainLibraryLocation(), targetTenantHoldingsRecord2.getString(PERMANENT_LOCATION_ID_KEY));

    // Verify related Items ownership updated
    Response sourceTenantItem1 = itemsClient.getById(firstItem.getId());
    List<JsonObject> targetTenantItems1 = collegeItemsClient.getMany(String.format("holdingsRecordId=%s", createHoldingsRecord1), 100);
    assertEquals(1, targetTenantItems1.size());

    JsonObject targetTenantItem1 = targetTenantItems1.getFirst();

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceTenantItem1.getStatusCode()));
    assertEquals(targetTenantItem1.getString(HOLDINGS_RECORD_ID), createHoldingsRecord1.toString());
    assertEquals(targetTenantItem1.getString(ID), firstItem.getId().toString());
    assertNull(targetTenantItem1.getString(PERMANENT_LOCATION_ID_KEY));
    assertNull(targetTenantItem1.getString(TEMPORARY_LOCATION_ID_KEY));

    Response sourceTenantItem2 = itemsClient.getById(secondItem.getId());
    List<JsonObject> targetTenantItems2 = collegeItemsClient.getMany(String.format("holdingsRecordId=%s", createHoldingsRecord2), 100);
    assertEquals(1, targetTenantItems1.size());

    JsonObject targetTenantItem2 = targetTenantItems2.getFirst();

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceTenantItem2.getStatusCode()));
    assertEquals(targetTenantItem2.getString(HOLDINGS_RECORD_ID), createHoldingsRecord2.toString());
    assertNull(targetTenantItem2.getString(PERMANENT_LOCATION_ID_KEY));
    assertNull(targetTenantItem2.getString(TEMPORARY_LOCATION_ID_KEY));
    assertEquals(secondItem.getId().toString(), targetTenantItem2.getString(ID));

    assertNotEquals(itemHrId, targetTenantItem2.getString("hrid"));
  }

  @Test
  public void canUpdateHoldingsOwnershipIfErrorUpdatingRelatedItemsToDifferentTenant() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Internal server error during item creation");
    collegeItemsClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.POST.name()));

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    collegeItemsClient.disableFailureEmulation();

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(HttpStatus.SC_BAD_REQUEST));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonArray notUpdatedEntities = postHoldingsUpdateOwnershipResponse.getJson().getJsonArray("notUpdatedEntities");
    assertThat(notUpdatedEntities.size(), is(1));

    JsonObject error = notUpdatedEntities.getJsonObject(0);
    assertThat(error.getString("entityId"), is(createHoldingsRecord1.toString()));
    assertTrue(error.getString("errorMessage").contains("Internal server exception: {\"message\":\"Internal server error during item creation\"}"));

    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 1);
    assertEquals(1, targetTenantHoldings.size());
    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));

    List<JsonObject> targetTenantItems = collegeItemsClient.getMany(String.format("holdingsRecordId=%s", targetTenantHoldingsRecord1.getString(ID)), 1);
    assertEquals(0, targetTenantItems.size());

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    assertThat(sourceTenantHoldingsRecord1.getStatusCode(), is(HttpStatus.SC_OK));
    assertEquals(instanceId.toString(), sourceTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));

    Response sourceTenantItem1 = itemsClient.getById(firstItem.getId());
    assertThat(sourceTenantItem1.getStatusCode(), is(HttpStatus.SC_OK));
    assertThat(sourceTenantItem1.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord1.toString()));
  }

  @Test
  public void canUpdateHoldingsOwnershipIfErrorDeletingRelatedItems() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Server error");
    itemsStorageClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.DELETE.name()));

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    itemsStorageClient.disableFailureEmulation();

    assertThat("Response status should be 400 Bad Request due to partial failure",
      postHoldingsUpdateOwnershipResponse.getStatusCode(), is(HttpStatus.SC_BAD_REQUEST));

    JsonArray notUpdatedEntities = postHoldingsUpdateOwnershipResponse.getJson().getJsonArray("notUpdatedEntities");
    assertThat("There should be exactly one not-updated entity", notUpdatedEntities.size(), is(1));
    assertThat(notUpdatedEntities.getJsonObject(0).getString("entityId"), equalTo(createHoldingsRecord1.toString()));
    assertThat(notUpdatedEntities.getJsonObject(0).getString("errorMessage"), containsString("Server error"));

    Response sourceHoldingsResponse = holdingsStorageClient.getById(createHoldingsRecord1);
    assertThat("Source holding should NOT be deleted due to failure in deleting its item",
      sourceHoldingsResponse.getStatusCode(), is(HttpStatus.SC_OK));

    List<JsonObject> targetHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId==%s", instanceId), 1);
    assertThat("One holding should be created in the target tenant", targetHoldings.size(), is(1));
    JsonObject targetHoldingsRecord1 = targetHoldings.getFirst();

    Response sourceItemResponse = itemsStorageClient.getById(firstItem.getId());
    assertThat("Source item should still exist in storage", sourceItemResponse.getStatusCode(), is(HttpStatus.SC_OK));
    assertThat("Source item should still be linked to the original holdings record ID", sourceItemResponse.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord1.toString()));

    List<JsonObject> targetItems = collegeItemsClient.getMany(String.format("holdingsRecordId==%s", targetHoldingsRecord1.getString(ID)), 1);
    assertThat("One item should be created in the target tenant", targetItems.size(), is(1));
    JsonObject targetItem1 = targetItems.getFirst();
    assertThat(targetItem1.getString(HOLDINGS_RECORD_ID), is(targetHoldingsRecord1.getString(ID)));
  }

  @Test
  public void shouldReportErrorsWhenOnlySomeRequestedHoldingsRecordsCouldNotBeUpdated() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = UUID.randomUUID();

    assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(400));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonArray notFoundIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.getJsonObject(0).getString("entityId"), equalTo(createHoldingsRecord2.toString()));
    assertThat(notFoundIds.getJsonObject(0).getString("errorMessage"),
      equalTo(String.format(HOLDINGS_RECORD_NOT_FOUND, createHoldingsRecord2, ApiTestSuite.TENANT_ID)));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(1, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    assertThat(instanceId.toString(), equalTo(targetTenantHoldingsRecord1.getString(INSTANCE_ID)));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);
    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
  }

  @Test
  public void shouldReportErrorWhenOnlySomeRequestedHoldingsRecordHasRelatedBoundWithParts() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    String itemHrId = "it0000001";

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
        .withHrid(itemHrId)
        .withBarcode("645398607546")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    JsonObject boundWithPart = makeObjectBoundWithPart(firstItem.getJson().getString("id"), createHoldingsRecord1.toString());
    boundWithPartsStorageClient.create(boundWithPart);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(400));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonArray notFoundIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.getJsonObject(0).getString("entityId"), equalTo(createHoldingsRecord1.toString()));
    assertThat(notFoundIds.getJsonObject(0).getString("errorMessage"),
      equalTo(String.format(HOLDING_BOUND_WITH_PARTS_ERROR, createHoldingsRecord1)));


    // Verify Holdings ownership updated
    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(1, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord = targetTenantHoldings.getFirst();

    assertEquals(HttpStatus.SC_OK, sourceTenantHoldingsRecord1.getStatusCode());

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord.getString(INSTANCE_ID));
    assertEquals(createHoldingsRecord2.toString(), targetTenantHoldingsRecord.getString(ID));

    // Verify related Items ownership updated
    Response sourceTenantItem1 = itemsClient.getById(firstItem.getId());
    List<JsonObject> targetTenantItems1 = collegeItemsClient.getMany(String.format("holdingsRecordId=%s", targetTenantHoldingsRecord.getString(ID)), 100);
    assertEquals(1, targetTenantItems1.size());

    JsonObject targetTenantItem = targetTenantItems1.getFirst();

    assertThat(HttpStatus.SC_OK, is(sourceTenantItem1.getStatusCode()));

    Response sourceTenantItem2 = itemsClient.getById(secondItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceTenantItem2.getStatusCode()));
    assertThat(targetTenantItem.getString(HOLDINGS_RECORD_ID), is(targetTenantHoldingsRecord.getString(ID)));
    assertEquals(secondItem.getId().toString(), targetTenantItem.getString(ID));
    assertNotEquals(itemHrId, targetTenantItem.getString("hrid"));
  }

  @Test
  public void shouldReportErrorsWhenOnlySomeRequestedHoldingsRecordsNotLinkedToSharedInstance() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId1 = UUID.randomUUID();
    JsonObject instance1 = smallAngryPlanet(instanceId1);

    InstanceApiClient.createInstance(okapiClient, instance1.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance1.put("source", FOLIO.getValue()));

    UUID instanceId2 = UUID.randomUUID();
    JsonObject instance2 = smallAngryPlanet(instanceId2);

    InstanceApiClient.createInstance(okapiClient, instance2.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId1);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId2);

    assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId1,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(400));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonArray notFoundIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.getJsonObject(0).getString("entityId"), equalTo(createHoldingsRecord2.toString()));
    assertThat(notFoundIds.getJsonObject(0).getString("errorMessage"),
      equalTo(String.format(HOLDINGS_RECORD_NOT_LINKED_TO_SHARED_INSTANCE, createHoldingsRecord2)));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId1), 100);
    assertEquals(1, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    assertThat(instanceId1.toString(), equalTo(targetTenantHoldingsRecord1.getString(INSTANCE_ID)));
  }

  @Test
  public void cannotUpdateHoldingsRecordsOwnershipToUnspecifiedInstance()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    JsonObject holdingsRecordUpdateOwnershipWithoutToInstanceId = new HoldingsRecordUpdateOwnershipRequestBuilder(null,
      new JsonArray(List.of(UUID.randomUUID())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

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
      new JsonArray(List.of(UUID.randomUUID())), UUID.randomUUID(), null).create();

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
      new JsonArray(List.of(UUID.randomUUID().toString())), UUID.randomUUID(), ApiTestSuite.TENANT_ID).create();

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
      new JsonArray(), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "holdingsRecordIds is a required field", "holdingsRecordIds", null
    ));
  }

  @Test
  public void cannotUpdateHoldingsRecordOwnershipToUnspecifiedTargetLocation()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    JsonObject holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds = new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
      new JsonArray(List.of(UUID.randomUUID().toString())), null, ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "targetLocationId is a required field", "targetLocationId", null
    ));
  }

  @Test
  public void cannotUpdateHoldingsRecordOwnershipIfTenantNotInConsortium()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    userTenantsClient.deleteAll();

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
      new JsonArray(List.of(UUID.randomUUID().toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

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
      new JsonArray(List.of(createHoldingsRecord1)), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

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
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

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

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Server error");
    collegeHoldingsStorageClient.emulateFailure(
      new EndpointFailureDescriptor()
      .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
      .setStatusCode(500)
      .setContentType("application/json")
      .setBody(expectedErrorResponse.toString())
      .setMethod(HttpMethod.POST.name()));

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    collegeHoldingsStorageClient.disableFailureEmulation();

    JsonArray notUpdatedEntitiesIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notUpdatedEntitiesIds.size(), is(1));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("entityId"), equalTo(createHoldingsRecord1.toString()));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("errorMessage"), containsString(expectedErrorResponse.toString()));

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(400));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    assertEquals(instanceId.toString(), sourceTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));
    assertEquals(HttpStatus.SC_NOT_FOUND, targetTenantHoldingsRecord1.getStatusCode());
  }

  @Test
  public void cannotUpdateHoldingsRecordOwnershipDueToHoldingsRecordDeleteError() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Server error");
    collegeHoldingsStorageClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.DELETE.name()));

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    collegeHoldingsStorageClient.disableFailureEmulation();

    JsonArray notUpdatedEntitiesIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notUpdatedEntitiesIds.size(), is(1));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("entityId"), equalTo(createHoldingsRecord1.toString()));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("errorMessage"), containsString(expectedErrorResponse.toString()));

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(400));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(1, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();

    assertEquals(instanceId.toString(), sourceTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));
  }

  @Test
  public void canUpdateHoldingsRecordOwnershipToDifferentInstanceWithExtraRedundantFields() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    JsonObject firstJsonHoldingsAsRequest = new HoldingRequestBuilder().forInstance(instanceId).create();
    final UUID createHoldingsRecord1 = holdingsStorageClient.create(firstJsonHoldingsAsRequest
        .put("holdingsItems", new JsonArray().add(new JsonObject().put(ID, UUID.randomUUID())).add(new JsonObject().put(ID, UUID.randomUUID())))
        .put("bareHoldingsItems", new JsonArray().add(new JsonObject().put(ID, UUID.randomUUID())).add(new JsonObject().put(ID, UUID.randomUUID()))))
      .getId();

    JsonObject secondJsonHoldingsAsRequest = new HoldingRequestBuilder().forInstance(instanceId).create();
    final UUID createHoldingsRecord2 = holdingsStorageClient.create(secondJsonHoldingsAsRequest
        .put("holdingsItems", new JsonArray().add(new JsonObject().put(ID, UUID.randomUUID())).add(new JsonObject().put(ID, UUID.randomUUID())))
        .put("bareHoldingsItems", new JsonArray().add(new JsonObject().put(ID, UUID.randomUUID())).add(new JsonObject().put(ID, UUID.randomUUID()))))
      .getId();

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(HttpStatus.SC_OK));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.getBody()).getJsonArray("notUpdatedEntities").size(), is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(2, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);
    JsonObject targetTenantHoldingsRecord2 = targetTenantHoldings.get(1);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getString(INSTANCE_ID));
  }

  @Test
  public void canUpdateOwnershipOfMarcHoldingAndMoveSrsRecord() throws Exception {

    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID holdingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    final JsonObject srsRecordToCreate = buildMarcSourceRecord(holdingsId);
    final String sourceSrsId = srsRecordToCreate.getString("id");
    sourceRecordStorageClient.create(srsRecordToCreate);

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(holdingsId.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);

    assertThat(response.getStatusCode(), is(HttpStatus.SC_OK));
    assertThat(new JsonObject(response.getBody()).getJsonArray("notUpdatedEntities").size(), is(0));

    //check that holding removed from source tenant
    Response sourceHoldingsResponse = holdingsStorageClient.getById(holdingsId);
    assertThat(sourceHoldingsResponse.getStatusCode(), is(HttpStatus.SC_NOT_FOUND));

    //check that holding created in target tenant
    List<JsonObject> targetHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 1);
    assertThat(targetHoldings.size(), is(1));
    assertThat(targetHoldings.getFirst().getString("id"), is(holdingsId.toString()));

    //check that SRS record from source tenant marked as DELETED
    Response sourceSrsResponse = sourceRecordStorageClient.getById(UUID.fromString(sourceSrsId));
    assertThat(sourceSrsResponse.getStatusCode(), is(HttpStatus.SC_NOT_FOUND));

    //check that SRS record created in target tenant
    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("matchedId==" + sourceSrsId, 1);
    assertThat(targetSrsRecords.size(), is(1));

    JsonObject targetSrsRecord = targetSrsRecords.getFirst();
    assertNotEquals(sourceSrsId, targetSrsRecord.getString("id"));
    assertEquals(sourceSrsId, targetSrsRecord.getString("matchedId"));
    assertEquals("MARC_HOLDING", targetSrsRecord.getString("recordType"));
  }

  @Test
  public void shouldFailMarcHoldingsAndMoveFolioHoldingWhenSnapshotCreationFails() throws Exception {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    InstanceApiClient.createInstance(okapiClient, instance.copy().put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.copy().put("source", FOLIO.getValue()));

    final UUID marcHoldingsId1 = holdingsStorageClient.create(
        new HoldingRequestBuilder().forInstance(instanceId).withMarcSource())
      .getId();
    final UUID marcHoldingsId2 = holdingsStorageClient.create(
        new HoldingRequestBuilder().forInstance(instanceId).withMarcSource())
      .getId();
    final UUID folioHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder().forInstance(instanceId))
      .getId();

    sourceRecordStorageClient.create(buildMarcSourceRecord(marcHoldingsId1));
    sourceRecordStorageClient.create(buildMarcSourceRecord(marcHoldingsId2));

    final JsonObject expectedErrorResponse = new JsonObject()
      .put("message", "Internal Server Error: Snapshot creation failed");
    collegeSourceRecordStorageClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(5).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.POST.name()));

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingsId1.toString(), marcHoldingsId2.toString(), folioHoldingsId.toString())),
      UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);

    collegeSourceRecordStorageClient.disableFailureEmulation();

    assertThat("Response status should be 400 Bad Request due to partial failure",
      response.getStatusCode(), is(HttpStatus.SC_BAD_REQUEST));

    JsonObject responseBody = response.getJson();
    JsonArray notUpdatedEntities = responseBody.getJsonArray("notUpdatedEntities");
    assertThat("Should have exactly two not-updated entities for MARC holdings", notUpdatedEntities.size(), is(2));

    List<String> failedEntityIds = notUpdatedEntities.stream()
      .map(obj -> ((JsonObject) obj).getString("entityId"))
      .toList();
    assertThat("Failed entities should be the two MARC holdings", failedEntityIds,
      containsInAnyOrder(marcHoldingsId1.toString(), marcHoldingsId2.toString()));

    //Check that FOLIO holding was moved to target tenant
    assertThat("FOLIO holding should be deleted from source tenant",
      holdingsStorageClient.getById(folioHoldingsId).getStatusCode(), is(HttpStatus.SC_NOT_FOUND));
    List<JsonObject> targetFolioHoldings = collegeHoldingsStorageClient
      .getMany(String.format("id==%s", folioHoldingsId), 1);
    assertThat("FOLIO holding should be created in target tenant", targetFolioHoldings.size(), is(1));

    //Check that MARC holdings still exist in source tenant
    assertThat("MARC holding 1 should still exist in source tenant",
      holdingsStorageClient.getById(marcHoldingsId1).getStatusCode(), is(HttpStatus.SC_OK));
    assertThat("MARC holding 2 should still exist in source tenant",
      holdingsStorageClient.getById(marcHoldingsId2).getStatusCode(), is(HttpStatus.SC_OK));

    //Check that MARC holdings created in target tenant
    List<JsonObject> targetMarcHoldings = collegeHoldingsStorageClient
      .getMany(String.format("id==(%s or %s)", marcHoldingsId1, marcHoldingsId2), 2);
    assertThat("MARC holdings are created in target tenant", targetMarcHoldings.size(), is(2));

    // Check that no SRS records created in target tenant
    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("recordType=MARC_HOLDING", 2);
    assertThat("No SRS records should be created in target tenant", targetSrsRecords.size(), is(0));
  }

  @Test
  public void shouldFailOneMarcAndMoveOtherHoldingsWhenSingleSrsRecordMoveFails() throws Exception {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    InstanceApiClient.createInstance(okapiClient, instance.copy().put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.copy().put("source", FOLIO.getValue()));

    final UUID successfulMarcId = holdingsStorageClient.create(
        new HoldingRequestBuilder().forInstance(instanceId).withHrId("ho00000000048").withMarcSource())
      .getId();
    final UUID failingMarcId = holdingsStorageClient.create(
        new HoldingRequestBuilder().forInstance(instanceId).withHrId("ho00000000049").withMarcSource())
      .getId();
    final UUID folioHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder().forInstance(instanceId).withHrId("ho00000000050"))
      .getId();

    final String successfulSrsId = sourceRecordStorageClient.create(
      buildMarcSourceRecord(successfulMarcId)).getJson().getString("id");

    sourceRecordStorageClient.create(buildMarcSourceRecord(failingMarcId));

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Internal Server Error: Record creation failed");
    collegeSourceRecordStorageClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(5).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.POST.name())
        .setBodyContains(failingMarcId.toString())
    );

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(successfulMarcId.toString(), failingMarcId.toString(), folioHoldingsId.toString())),
      UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);

    collegeSourceRecordStorageClient.disableFailureEmulation();

    assertThat("Response status should be 400 Bad Request due to partial failure",
      response.getStatusCode(), is(HttpStatus.SC_BAD_REQUEST));

    JsonObject responseBody = response.getJson();
    JsonArray notUpdatedEntities = responseBody.getJsonArray("notUpdatedEntities");
    assertThat("Should have exactly one not-updated entity", notUpdatedEntities.size(), is(1));
    assertThat("Failed entity should be the failing MARC holding",
      notUpdatedEntities.getJsonObject(0).getString("entityId"), is(failingMarcId.toString()));

    // Check that FOLIO holding was moved to target tenant
    assertThat("FOLIO holding should be deleted from source tenant",
      holdingsStorageClient.getById(folioHoldingsId).getStatusCode(), is(HttpStatus.SC_NOT_FOUND));
    assertThat("FOLIO holding should be created in target tenant",
      collegeHoldingsStorageClient.getMany(String.format("id==%s", folioHoldingsId), 1).size(), is(1));

    // Check that successful MARC holding was moved to target tenant
    assertThat("Successful MARC holding should be deleted from source tenant",
      holdingsStorageClient.getById(successfulMarcId).getStatusCode(), is(HttpStatus.SC_NOT_FOUND));
    assertThat("Successful MARC holding should be created in target tenant",
      collegeHoldingsStorageClient.getMany(String.format("id==%s", successfulMarcId), 1).size(), is(1));
    Response sourceSrsForSuccess = sourceRecordStorageClient.getById(UUID.fromString(successfulSrsId));
    assertThat("Source SRS for successful holding should be marked as DELETED",
      sourceSrsForSuccess.getStatusCode(), is(HttpStatus.SC_NOT_FOUND));

    // Check that failing MARC holding still exists in source tenant
    assertThat("Failing MARC holding should still exist in source tenant",
      holdingsStorageClient.getById(failingMarcId).getStatusCode(), is(HttpStatus.SC_OK));
    assertThat("Failing MARC holding SHOULD BE created in target tenant despite SRS failure",
      collegeHoldingsStorageClient.getMany(String.format("id==%s", failingMarcId), 1).size(), is(1));
    assertThat("SRS record for failing MARC holding should NOT be created in target tenant",
      collegeSourceRecordStorageClient.getMany(String.format("externalIdsHolder.holdingsId==%s", failingMarcId), 1).size(), is(0));

    // Check that one SRS record created in target tenant for successful holding
    // and it corresponds to the successful holding
    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("recordType==MARC_HOLDING", 2);
    assertThat("Exactly one SRS record should be created in target tenant", targetSrsRecords.size(), is(1));
    assertThat("The created SRS record should correspond to the successful holding",
      targetSrsRecords.getFirst().getString("matchedId"), is(successfulSrsId));
  }

  @Test
  public void shouldReturn400AndNotUpdateMarcHoldingWhenSrsRecordIsNotFound() throws Exception {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    InstanceApiClient.createInstance(okapiClient, instance.copy().put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.copy().put("source", FOLIO.getValue()));

    final UUID failingHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    final UUID successfulHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
      )
      .getId();

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(failingHoldingsId.toString(), successfulHoldingsId.toString())),
      UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);
    assertThat(response.getStatusCode(), is(HttpStatus.SC_BAD_REQUEST));

    JsonObject responseBody = response.getJson();
    JsonArray notUpdatedEntities = responseBody.getJsonArray("notUpdatedEntities");
    assertThat(notUpdatedEntities.size(), is(1));

    JsonObject failedEntity = notUpdatedEntities.getJsonObject(0);
    assertThat(failedEntity.getString("entityId"), is(failingHoldingsId.toString()));
    assertThat(failedEntity.getString("errorMessage"), containsString("Failed to fetch MARC source record"));

    // marc holding without SRS record should not be moved to target tenant
    Response failingSourceHoldingsResponse = holdingsStorageClient.getById(failingHoldingsId);
    assertThat(failingSourceHoldingsResponse.getStatusCode(), is(HttpStatus.SC_OK));

    // FOLIO holding should br moved to target tenant
    Response successfulSourceHoldingsResponse = holdingsStorageClient.getById(successfulHoldingsId);
    assertThat(successfulSourceHoldingsResponse.getStatusCode(), is(HttpStatus.SC_NOT_FOUND));

    // check that one holding created in target tenant
    List<JsonObject> targetHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 1);
    assertThat(targetHoldings.size(), is(1));
  }

  @Test
  public void shouldDoNothingAndReportAllAsNotUpdatedWhenNoValidHoldingsRemainAfterValidation() throws Exception {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    InstanceApiClient.createInstance(okapiClient, instance.copy().put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.copy().put("source", FOLIO.getValue()));

    // MARC holding without SRS record. Should be filtered by validateHoldingsRecordsMarcSource.
    final UUID marcHoldingId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    // FOLIO hodling with bound-with. Should be filtered by validateHoldingsRecordsBoundWith.
    final UUID boundWithHoldingId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
      )
      .getId();

    // Creation item and bound-with-part for holding 2
    final var itemForBoundWith = itemsClient.create(
      new ItemRequestBuilder().forHolding(boundWithHoldingId));
    JsonObject boundWithPart = makeObjectBoundWithPart(itemForBoundWith.getId().toString(), boundWithHoldingId.toString());
    boundWithPartsStorageClient.create(boundWithPart);

    //ACTION
    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingId.toString(), boundWithHoldingId.toString())),
      UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);
    assertThat(response.getStatusCode(), is(HttpStatus.SC_BAD_REQUEST));

    // Should return 2 not-updated holdings
    JsonObject responseBody = response.getJson();
    JsonArray notUpdatedEntities = responseBody.getJsonArray("notUpdatedEntities");
    assertThat("There should be two not-updated entities", notUpdatedEntities.size(), is(2));

    List<JsonObject> errors = notUpdatedEntities.stream()
      .map(obj -> (JsonObject) obj)
      .toList();

    // Check errors for MARC holding without SRS record
    assertTrue("Should contain error for MARC holding without SRS",
      errors.stream().anyMatch(error ->
        error.getString("entityId").equals(marcHoldingId.toString()) &&
          error.getString("errorMessage").contains("Failed to fetch MARC source record")
      ));

    // Check errors for bound-with holding
    assertTrue("Should contain error for bound-with holding",
      errors.stream().anyMatch(error ->
        error.getString("entityId").equals(boundWithHoldingId.toString()) &&
          error.getString("errorMessage").equals(String.format(HOLDING_BOUND_WITH_PARTS_ERROR, boundWithHoldingId))
      ));

    // Check that no holdings were moved to target tenant
    assertThat(holdingsStorageClient.getById(marcHoldingId).getStatusCode(), is(HttpStatus.SC_OK));
    assertThat(holdingsStorageClient.getById(boundWithHoldingId).getStatusCode(), is(HttpStatus.SC_OK));

    // Check that no holdings were created in target tenant
    List<JsonObject> targetHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId==%s", instanceId), 2);
    assertThat("No holdings should be created in the target tenant", targetHoldings.size(), is(0));
  }

  @Test
  public void shouldReturn400AndReportErrorWhenSnapshotCreationFailsForMarcHolding() throws Exception {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));

    // Create MARC holding which requires snapshot creation
    final UUID marcHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    // Create corresponding SRS record
    final JsonObject srsRecordToCreate = buildMarcSourceRecord(marcHoldingsId);
    sourceRecordStorageClient.create(srsRecordToCreate);

    // Create regular FOLIO holding (no snapshot needed)
    final UUID folioHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
      )
      .getId();

    // Emulate failure on snapshot creation endpoint in target tenant (college)
    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Snapshot creation failed");
    collegeSourceRecordStorageClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.POST.name()));

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingsId.toString(), folioHoldingsId.toString())),
      UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);

    collegeSourceRecordStorageClient.disableFailureEmulation();

    // Verify that the operation returns 400 due to snapshot creation failure
    assertThat(response.getStatusCode(), is(HttpStatus.SC_BAD_REQUEST));

    JsonObject responseBody = response.getJson();
    JsonArray notUpdatedEntities = responseBody.getJsonArray("notUpdatedEntities");
    assertThat("Should have exactly one not-updated entity for MARC holding", notUpdatedEntities.size(), is(1));

    JsonObject failedEntity = notUpdatedEntities.getJsonObject(0);
    assertThat("Failed entity should be the MARC holding", failedEntity.getString("entityId"), is(marcHoldingsId.toString()));
    assertThat("Error message should indicate snapshot creation failure",
      failedEntity.getString("errorMessage"), containsString("Failed to post SRS record to target tenant=college: {\"message\":\"Snapshot creation failed\"}"));

    // Verify MARC holding remains in source tenant due to SRS failure (partial failure)
    Response marcHoldingResponse = holdingsStorageClient.getById(marcHoldingsId);
    assertThat("MARC holding should still exist in source tenant due to SRS failure", marcHoldingResponse.getStatusCode(), is(HttpStatus.SC_OK));

    // Verify FOLIO holding was successfully moved to target tenant
    Response folioHoldingResponse = holdingsStorageClient.getById(folioHoldingsId);
    assertThat("FOLIO holding should be deleted from source tenant", folioHoldingResponse.getStatusCode(), is(HttpStatus.SC_NOT_FOUND));

    // Both holdings should be created in target tenant (holdings migration succeeds, SRS migration fails)
    List<JsonObject> targetHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId==%s", instanceId), 3);
    assertThat("Both holdings should be created in target tenant", targetHoldings.size(), is(2));

    List<String> targetHoldingIds = targetHoldings.stream().map(h -> h.getString("id")).toList();
    assertTrue("MARC holding should be in target tenant", targetHoldingIds.contains(marcHoldingsId.toString()));
    assertTrue("FOLIO holding should be in target tenant", targetHoldingIds.contains(folioHoldingsId.toString()));
  }

   private JsonObject buildMarcSourceRecord(UUID holdingsId) {
    final var srsId = UUID.randomUUID();
    return new JsonObject()
      .put("id", srsId.toString())
      .put("snapshotId", UUID.randomUUID().toString())
      .put("matchedId", srsId.toString())
      .put("recordType", "MARC_HOLDING")
      .put("externalIdsHolder", new JsonObject().put("holdingsId", holdingsId.toString()))
      .put("parsedRecord", new JsonObject()
        .put("id", srsId.toString())
        .put("content", new JsonObject()
          .put("leader", "00000nu  a2200000   4500")
          .put("fields", new JsonArray()
            .add(new JsonObject().put("852", new JsonObject()
              .put("subfields", new JsonArray().add(new JsonObject().put("h", "Some call number")))
              .put("ind1", " ")
              .put("ind2", " "))))
        )
      );
  }

  private Response updateHoldingsRecordsOwnership(JsonObject holdingsRecordUpdateOwnershipRequestBody) throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var postHoldingRecordsUpdateOwnershipCompleted = okapiClient.post(
      ApiRoot.updateHoldingsRecordsOwnership(), holdingsRecordUpdateOwnershipRequestBody);
    return postHoldingRecordsUpdateOwnershipCompleted.toCompletableFuture().get(30, TimeUnit.SECONDS);
  }

  private UUID createHoldingForInstance(UUID instanceId) {
    return holdingsStorageClient.create(new HoldingRequestBuilder().withHrId("hol0000001").forInstance(instanceId))
      .getId();
  }
}
