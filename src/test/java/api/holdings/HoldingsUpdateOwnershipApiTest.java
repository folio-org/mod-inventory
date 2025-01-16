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
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
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

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.getBody()).getJsonArray("notUpdatedEntities").size(), is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(2, targetTenantHoldings.size());

    var targetTenantHoldingIds = targetTenantHoldings.stream().map(object -> object.getString(ID))
      .toList();

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.get(0);

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

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
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

    JsonObject targetTenantItem1 = targetTenantItems1.get(0);

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceTenantItem1.getStatusCode()));
    assertEquals(targetTenantItem1.getString(HOLDINGS_RECORD_ID), createHoldingsRecord1.toString());
    assertEquals(targetTenantItem1.getString(ID), firstItem.getId().toString());

    Response sourceTenantItem2 = itemsClient.getById(secondItem.getId());
    List<JsonObject> targetTenantItems2 = collegeItemsClient.getMany(String.format("holdingsRecordId=%s", createHoldingsRecord2), 100);
    assertEquals(1, targetTenantItems1.size());

    JsonObject targetTenantItem2 = targetTenantItems2.get(0);

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceTenantItem2.getStatusCode()));
    assertEquals(targetTenantItem2.getString(HOLDINGS_RECORD_ID), createHoldingsRecord2.toString());
    assertEquals(secondItem.getId().toString(), targetTenantItem2.getString(ID));

    assertNotEquals(targetTenantItem2.getString("hrid"), itemHrId);
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
      new JsonArray(List.of(createHoldingsRecord1.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    collegeItemsClient.disableFailureEmulation();
    holdingsStorageClient.disableFailureEmulation();

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));

    JsonArray notUpdatedEntitiesIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notUpdatedEntitiesIds.size(), is(1));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("entityId"), equalTo(createHoldingsRecord1.toString()));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("errorMessage"), containsString(expectedErrorResponse.toString()));

    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    // Verify Holdings ownership updated
    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(1, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.get(0);

    assertEquals(instanceId.toString(), sourceTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));

    // Verify related Item ownership not updated
    Response sourceTenantItem1 = itemsClient.getById(firstItem.getId());
    List<JsonObject> targetTenantItems1 = collegeItemsClient.getMany(String.format("holdingsRecordId=%s", targetTenantHoldingsRecord1.getString(ID)), 100);

    assertThat(sourceTenantItem1.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord1.toString()));
    assertEquals(0, targetTenantItems1.size());
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
    collegeItemsClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(2).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.DELETE.name()));

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString())), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse = updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    collegeItemsClient.disableFailureEmulation();

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));

    JsonArray notUpdatedEntitiesIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notUpdatedEntitiesIds.size(), is(1));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("entityId"), equalTo(createHoldingsRecord1.toString()));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("errorMessage"), containsString(expectedErrorResponse.toString()));

    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    // Verify Holdings ownership updated
    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(1, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.get(0);

    assertEquals(instanceId.toString(), sourceTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));

    // Verify related Item ownership not updated
    Response sourceTenantItem1 = itemsClient.getById(firstItem.getId());
    List<JsonObject> targetTenantItems1 = collegeItemsClient.getMany(String.format("holdingsRecordId=%s", targetTenantHoldingsRecord1.getString(ID)), 100);
    assertEquals(1, targetTenantItems1.size());

    JsonObject targetTenantItem1 = targetTenantItems1.get(0);

    assertThat(sourceTenantItem1.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord1.toString()));
    assertThat(targetTenantItem1.getString(HOLDINGS_RECORD_ID), is(targetTenantHoldingsRecord1.getString(ID)));
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

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
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

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.get(0);

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

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
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

    JsonObject targetTenantHoldingsRecord = targetTenantHoldings.get(0);

    assertEquals(HttpStatus.SC_OK, sourceTenantHoldingsRecord1.getStatusCode());

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord.getString(INSTANCE_ID));
    assertEquals(createHoldingsRecord2.toString(), targetTenantHoldingsRecord.getString(ID));

    // Verify related Items ownership updated
    Response sourceTenantItem1 = itemsClient.getById(firstItem.getId());
    List<JsonObject> targetTenantItems1 = collegeItemsClient.getMany(String.format("holdingsRecordId=%s", targetTenantHoldingsRecord.getString(ID)), 100);
    assertEquals(1, targetTenantItems1.size());

    JsonObject targetTenantItem = targetTenantItems1.get(0);

    assertThat(HttpStatus.SC_OK, is(sourceTenantItem1.getStatusCode()));

    Response sourceTenantItem2 = itemsClient.getById(secondItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceTenantItem2.getStatusCode()));
    assertThat(targetTenantItem.getString(HOLDINGS_RECORD_ID), is(targetTenantHoldingsRecord.getString(ID)));
    assertEquals(secondItem.getId().toString(), targetTenantItem.getString(ID));
    assertNotEquals(targetTenantItem.getString("hrid"), itemHrId);
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

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
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

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.get(0);

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

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
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

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(1, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.get(0);

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

    assertThat(postHoldingsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.getBody()).getJsonArray("notUpdatedEntities").size(), is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings = collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(2, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.get(0);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.getStatusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);
    JsonObject targetTenantHoldingsRecord2 = targetTenantHoldings.get(1);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.getStatusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getString(INSTANCE_ID));
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
