package api.holdings;

import static api.ApiTestSuite.createConsortiumTenant;
import static api.ApiTestSuite.getMainLibraryLocation;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;
import static org.folio.inventory.resources.UpdateOwnershipApi.HOLDINGS_RECORD_NOT_FOUND;
import static org.folio.inventory.resources.UpdateOwnershipApi.HOLDINGS_RECORD_NOT_LINKED_TO_SHARED_INSTANCE;
import static org.folio.inventory.resources.UpdateOwnershipApi.HOLDING_BOUND_WITH_PARTS_ERROR;
import static org.folio.inventory.support.ItemUtil.HOLDINGS_RECORD_ID;
import static org.folio.inventory.support.ItemUtil.PERMANENT_LOCATION_ID_KEY;
import static org.folio.inventory.support.ItemUtil.TEMPORARY_LOCATION_ID_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static support.FutureAssistance.getOnCompletion;
import static support.fixtures.InstanceFixture.smallAngryPlanet;
import static support.matchers.ResponseMatchers.hasNotUpdatedEntity;
import static support.matchers.ResponseMatchers.hasStatusAndJsonBody;
import static support.matchers.ResponseMatchers.hasValidationError;

import api.ApiTestSuite;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.apache.http.HttpStatus;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.support.http.client.Response;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import support.ApiRoot;
import support.ConsortiumApiTests;
import support.InstanceApiClient;
import support.builders.BoundWithPartRequestBuilder;
import support.builders.HoldingRequestBuilder;
import support.builders.HoldingsRecordUpdateOwnershipRequestBuilder;
import support.builders.ItemRequestBuilder;
import support.fakes.EndpointFailureDescriptor;
import support.fixtures.MarcSourceRecordFixture;
import support.http.ResourceClient;
import support.http.StorageInterfaceUrls;

public class HoldingsUpdateOwnershipApiTest extends ConsortiumApiTests {

  private static final String INSTANCE_ID = "instanceId";
  private static final String ID = "id";
  private static final String MAIN_LIBRARY_LOCATION_CODE = "NU/JC/DL/ML";

  @BeforeEach
  @SneakyThrows
  void cleanUpAdditionalResources() {
    holdingsStorageClient.deleteAll();
    collegeHoldingsStorageClient.deleteAll();

    itemsClient.deleteAll();
    collegeItemsClient.deleteAll();
    boundWithPartsStorageClient.deleteAll();

    sourceRecordStorageClient.deleteAll();
    collegeSourceRecordStorageClient.deleteAll();
  }

  @Test
  @SneakyThrows
  void canUpdateHoldingsOwnershipToDifferentTenant() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    createSharedInstanceAcrossTenants(instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())),
      UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.statusCode(), is(HttpStatus.SC_OK));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.body()).getJsonArray("notUpdatedEntities").size(),
      is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.contentType(), containsString(
      HttpHeaderValues.APPLICATION_JSON.toString()));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(2, targetTenantHoldings.size());

    var targetTenantHoldingIds = targetTenantHoldings.stream().map(object -> object.getString(ID))
      .toList();

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.statusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));
    assertEquals(getMainLibraryLocation(), targetTenantHoldingsRecord1.getString(PERMANENT_LOCATION_ID_KEY));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);
    JsonObject targetTenantHoldingsRecord2 = targetTenantHoldings.get(1);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.statusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getString(INSTANCE_ID));
    assertEquals(getMainLibraryLocation(), targetTenantHoldingsRecord2.getString(PERMANENT_LOCATION_ID_KEY));
    assertNull(targetTenantHoldingsRecord2.getString("hrid"));
    assertTrue(targetTenantHoldingIds.contains(createHoldingsRecord1.toString()));
    assertTrue(targetTenantHoldingIds.contains(createHoldingsRecord2.toString()));
  }

  @Test
  @SneakyThrows
  void canUpdateHoldingsOwnershipWithRelatedItemsToDifferentTenant() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    String itemHrId = "it0000001";
    String locationId = UUID.randomUUID().toString();
    JsonObject location = new JsonObject().put("id", locationId).put("name", "location");

    createSharedInstanceAcrossTenants(instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withOrder(10)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value())
        .withTemporaryLocation(location)
        .withPermanentLocation(location));

    final var secondItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord2)
        .withOrder(20)
        .withHrid(itemHrId)
        .withBarcode("645398607546")
        .withStatus(ItemStatusName.AVAILABLE.value())
        .withTemporaryLocation(location)
        .withPermanentLocation(location));

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())),
      UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.statusCode(), is(HttpStatus.SC_OK));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.body()).getJsonArray("notUpdatedEntities").size(),
      is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.contentType(), containsString(
      HttpHeaderValues.APPLICATION_JSON.toString()));

    // Verify Holdings ownership updated
    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(2, targetTenantHoldings.size());

    var targetTenantHoldingIds = targetTenantHoldings.stream().map(object -> object.getString(ID))
      .toList();

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.get(1);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.statusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));
    assertTrue(targetTenantHoldingIds.contains(createHoldingsRecord1.toString()));
    assertEquals(getMainLibraryLocation(), targetTenantHoldingsRecord1.getString(PERMANENT_LOCATION_ID_KEY));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);
    JsonObject targetTenantHoldingsRecord2 = targetTenantHoldings.get(1);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.statusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getString(INSTANCE_ID));
    assertTrue(targetTenantHoldingIds.contains(createHoldingsRecord2.toString()));
    assertEquals(getMainLibraryLocation(), targetTenantHoldingsRecord2.getString(PERMANENT_LOCATION_ID_KEY));

    // Verify related Items ownership updated
    Response sourceTenantItem1 = itemsClient.getById(firstItem.getId());
    List<JsonObject> targetTenantItems1 =
      collegeItemsClient.getMany(String.format("holdingsRecordId=%s", createHoldingsRecord1), 100);
    assertEquals(1, targetTenantItems1.size());

    JsonObject targetTenantItem1 = targetTenantItems1.getFirst();

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceTenantItem1.statusCode()));
    assertEquals(targetTenantItem1.getString(HOLDINGS_RECORD_ID), createHoldingsRecord1.toString());
    assertEquals(targetTenantItem1.getString(ID), firstItem.getId().toString());
    assertNull(targetTenantItem1.getString(PERMANENT_LOCATION_ID_KEY));
    assertNull(targetTenantItem1.getString(TEMPORARY_LOCATION_ID_KEY));
    assertEquals(10, (int) targetTenantItem1.getInteger(Item.ORDER_KEY));

    Response sourceTenantItem2 = itemsClient.getById(secondItem.getId());
    List<JsonObject> targetTenantItems2 =
      collegeItemsClient.getMany(String.format("holdingsRecordId=%s", createHoldingsRecord2), 100);
    assertEquals(1, targetTenantItems1.size());

    JsonObject targetTenantItem2 = targetTenantItems2.getFirst();

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceTenantItem2.statusCode()));
    assertEquals(targetTenantItem2.getString(HOLDINGS_RECORD_ID), createHoldingsRecord2.toString());
    assertNull(targetTenantItem2.getString(PERMANENT_LOCATION_ID_KEY));
    assertNull(targetTenantItem2.getString(TEMPORARY_LOCATION_ID_KEY));
    assertEquals(secondItem.getId().toString(), targetTenantItem2.getString(ID));
    assertEquals(20, (int) targetTenantItem2.getInteger(Item.ORDER_KEY));

    assertNotEquals(itemHrId, targetTenantItem2.getString("hrid"));
  }

  @Test
  @SneakyThrows
  void canUpdateHoldingsOwnershipIfErrorUpdatingRelatedItemsToDifferentTenant() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    createSharedInstanceAcrossTenants(instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final JsonObject expectedErrorResponse =
      new JsonObject().put("message", "Internal server error during item creation");
    collegeItemsClient.emulateFailure(500, HttpMethod.POST.name(), expectedErrorResponse.toString());

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString())), UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    collegeItemsClient.disableFailureEmulation();

    assertThat(postHoldingsUpdateOwnershipResponse, hasStatusAndJsonBody(HttpStatus.SC_BAD_REQUEST));

    assertThat(postHoldingsUpdateOwnershipResponse, hasNotUpdatedEntity(createHoldingsRecord1.toString(),
      "Internal server exception: {\"message\":\"Internal server error during item creation\"}"));

    List<JsonObject> targetTenantHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 1);
    assertEquals(1, targetTenantHoldings.size());
    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));

    List<JsonObject> targetTenantItems =
      collegeItemsClient.getMany(String.format("holdingsRecordId=%s", targetTenantHoldingsRecord1.getString(ID)), 1);
    assertEquals(0, targetTenantItems.size());

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    assertThat(sourceTenantHoldingsRecord1.statusCode(), is(HttpStatus.SC_OK));
    assertEquals(instanceId.toString(), sourceTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));

    Response sourceTenantItem1 = itemsClient.getById(firstItem.getId());
    assertThat(sourceTenantItem1.statusCode(), is(HttpStatus.SC_OK));
    assertThat(sourceTenantItem1.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord1.toString()));
  }

  @Test
  @SneakyThrows
  void canUpdateHoldingsOwnershipIfErrorDeletingRelatedItems() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    createSharedInstanceAcrossTenants(instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Server error");
    itemsStorageClient.emulateFailure(500, HttpMethod.DELETE.name(), expectedErrorResponse.toString());

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString())), UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    itemsStorageClient.disableFailureEmulation();

    assertThat("Response status should be 400 Bad Request due to partial failure",
      postHoldingsUpdateOwnershipResponse.statusCode(), is(HttpStatus.SC_BAD_REQUEST));

    assertThat(postHoldingsUpdateOwnershipResponse,
      hasNotUpdatedEntity(createHoldingsRecord1.toString(), "Server error"));

    Response sourceHoldingsResponse = holdingsStorageClient.getById(createHoldingsRecord1);
    assertThat("Source holding should NOT be deleted due to failure in deleting its item",
      sourceHoldingsResponse.statusCode(), is(HttpStatus.SC_OK));

    List<JsonObject> targetHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId==%s", instanceId), 1);
    assertThat("One holding should be created in the target tenant", targetHoldings.size(), is(1));
    JsonObject targetHoldingsRecord1 = targetHoldings.getFirst();

    Response sourceItemResponse = itemsStorageClient.getById(firstItem.getId());
    assertThat("Source item should still exist in storage", sourceItemResponse.statusCode(), is(HttpStatus.SC_OK));
    assertThat("Source item should still be linked to the original holdings record ID",
      sourceItemResponse.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord1.toString()));

    List<JsonObject> targetItems =
      collegeItemsClient.getMany(String.format("holdingsRecordId==%s", targetHoldingsRecord1.getString(ID)), 1);
    assertThat("One item should be created in the target tenant", targetItems.size(), is(1));
    JsonObject targetItem1 = targetItems.getFirst();
    assertThat(targetItem1.getString(HOLDINGS_RECORD_ID), is(targetHoldingsRecord1.getString(ID)));
  }

  @Test
  @SneakyThrows
  void shouldReportErrorsWhenOnlySomeRequestedHoldingsRecordsCouldNotBeUpdated() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    createSharedInstanceAcrossTenants(instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = UUID.randomUUID();

    assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())),
      UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse, hasStatusAndJsonBody(400));

    JsonArray notFoundIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.getJsonObject(0).getString("entityId"), equalTo(createHoldingsRecord2.toString()));
    assertThat(notFoundIds.getJsonObject(0).getString("errorMessage"),
      equalTo(String.format(HOLDINGS_RECORD_NOT_FOUND, createHoldingsRecord2, ApiTestSuite.TENANT_ID)));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(1, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.statusCode());
    assertThat(instanceId.toString(), equalTo(targetTenantHoldingsRecord1.getString(INSTANCE_ID)));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);
    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.statusCode());
  }

  @Test
  @SneakyThrows
  void shouldReportErrorWhenOnlySomeRequestedHoldingsRecordHasRelatedBoundWithParts() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    String itemHrId = "it0000001";

    createSharedInstanceAcrossTenants(instance);

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

    JsonObject boundWithPart =
      new BoundWithPartRequestBuilder(firstItem.getJson().getString("id"), createHoldingsRecord1.toString()).create();
    boundWithPartsStorageClient.create(boundWithPart);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())),
      UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse, hasStatusAndJsonBody(400));

    JsonArray notFoundIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.getJsonObject(0).getString("entityId"), equalTo(createHoldingsRecord1.toString()));
    assertThat(notFoundIds.getJsonObject(0).getString("errorMessage"),
      equalTo(String.format(HOLDING_BOUND_WITH_PARTS_ERROR, createHoldingsRecord1)));

    // Verify Holdings ownership updated
    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(1, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord = targetTenantHoldings.getFirst();

    assertEquals(HttpStatus.SC_OK, sourceTenantHoldingsRecord1.statusCode());

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.statusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord.getString(INSTANCE_ID));
    assertEquals(createHoldingsRecord2.toString(), targetTenantHoldingsRecord.getString(ID));

    // Verify related Items ownership updated
    Response sourceTenantItem1 = itemsClient.getById(firstItem.getId());
    List<JsonObject> targetTenantItems1 =
      collegeItemsClient.getMany(String.format("holdingsRecordId=%s", targetTenantHoldingsRecord.getString(ID)), 100);
    assertEquals(1, targetTenantItems1.size());

    JsonObject targetTenantItem = targetTenantItems1.getFirst();

    assertThat(HttpStatus.SC_OK, is(sourceTenantItem1.statusCode()));

    Response sourceTenantItem2 = itemsClient.getById(secondItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceTenantItem2.statusCode()));
    assertThat(targetTenantItem.getString(HOLDINGS_RECORD_ID), is(targetTenantHoldingsRecord.getString(ID)));
    assertEquals(secondItem.getId().toString(), targetTenantItem.getString(ID));
    assertNotEquals(itemHrId, targetTenantItem.getString("hrid"));
  }

  @Test
  @SneakyThrows
  void shouldReportErrorsWhenOnlySomeRequestedHoldingsRecordsNotLinkedToSharedInstance() {
    UUID instanceId1 = UUID.randomUUID();
    JsonObject instance1 = smallAngryPlanet(instanceId1);

    createSharedInstanceAcrossTenants(instance1);

    UUID instanceId2 = UUID.randomUUID();
    JsonObject instance2 = smallAngryPlanet(instanceId2);

    InstanceApiClient.createInstance(okapiClient, instance2.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId1);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId2);

    assertNotEquals(createHoldingsRecord1, createHoldingsRecord2);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId1,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())),
      UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse, hasStatusAndJsonBody(400));

    JsonArray notFoundIds = postHoldingsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.getJsonObject(0).getString("entityId"), equalTo(createHoldingsRecord2.toString()));
    assertThat(notFoundIds.getJsonObject(0).getString("errorMessage"),
      equalTo(String.format(HOLDINGS_RECORD_NOT_LINKED_TO_SHARED_INSTANCE, createHoldingsRecord2)));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId1), 100);
    assertEquals(1, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.statusCode());
    assertThat(instanceId1.toString(), equalTo(targetTenantHoldingsRecord1.getString(INSTANCE_ID)));
  }

  @Test
  void cannotUpdateHoldingsRecordsOwnershipToUnspecifiedInstance() {
    JsonObject holdingsRecordUpdateOwnershipWithoutToInstanceId = new HoldingsRecordUpdateOwnershipRequestBuilder(null,
      new JsonArray(List.of(UUID.randomUUID())), UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutToInstanceId);

    assertThat(postHoldingsUpdateOwnershipResponse, hasStatusAndJsonBody(422));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "toInstanceId is a required field", "toInstanceId", null
    ));
  }

  @Test
  void cannotUpdateHoldingsRecordsOwnershipToUnspecifiedTenant() {
    JsonObject holdingsRecordUpdateOwnershipWithoutTenantId =
      new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
        new JsonArray(List.of(UUID.randomUUID())), UUID.randomUUID(), null).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutTenantId);

    assertThat(postHoldingsUpdateOwnershipResponse, hasStatusAndJsonBody(422));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "targetTenantId is a required field", "targetTenantId", null
    ));
  }

  @Test
  void cannotUpdateHoldingsRecordOwnershipToSameTenant() {
    JsonObject holdingsRecordUpdateOwnershipRequestBody =
      new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
        new JsonArray(List.of(UUID.randomUUID().toString())), UUID.randomUUID(), ApiTestSuite.TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse, hasStatusAndJsonBody(422));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "targetTenantId field cannot be equal to source tenant id", "targetTenantId", ApiTestSuite.TENANT_ID
    ));
  }

  @Test
  void cannotUpdateUnspecifiedHoldingsRecordsOwnership() {
    JsonObject holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds =
      new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
        new JsonArray(), UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds);

    assertThat(postHoldingsUpdateOwnershipResponse, hasStatusAndJsonBody(422));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "holdingsRecordIds is a required field", "holdingsRecordIds", null
    ));
  }

  @Test
  void cannotUpdateHoldingsRecordOwnershipToUnspecifiedTargetLocation() {
    JsonObject holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds =
      new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
        new JsonArray(List.of(UUID.randomUUID().toString())), null, ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds);

    assertThat(postHoldingsUpdateOwnershipResponse, hasStatusAndJsonBody(422));

    assertThat(postHoldingsUpdateOwnershipResponse, hasValidationError(
      "targetLocationId is a required field", "targetLocationId", null
    ));
  }

  @Test
  @SneakyThrows
  void cannotUpdateHoldingsRecordOwnershipIfTenantNotInConsortium() {
    userTenantsClient.deleteAll();

    JsonObject holdingsRecordUpdateOwnershipRequestBody =
      new HoldingsRecordUpdateOwnershipRequestBuilder(UUID.randomUUID(),
        new JsonArray(List.of(UUID.randomUUID().toString())), UUID.fromString(getMainLibraryLocation()),
        ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.statusCode(), is(400));

    assertThat(postHoldingsUpdateOwnershipResponse.body(), containsString("tenant is not in consortia"));
    createConsortiumTenant();
  }

  @Test
  void cannotUpdateHoldingsRecordOwnershipOfNonExistedInstance() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    UUID invalidInstanceId = UUID.randomUUID();

    InstanceApiClient.createInstance(okapiClient, instance);
    InstanceApiClient.createInstance(consortiumOkapiClient, instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);

    JsonObject holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds =
      new HoldingsRecordUpdateOwnershipRequestBuilder(invalidInstanceId,
        new JsonArray(List.of(createHoldingsRecord1)), UUID.fromString(getMainLibraryLocation()),
        ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipWithoutHoldingsRecordIds);

    assertThat(postHoldingsUpdateOwnershipResponse.statusCode(), is(404));

    assertThat(postHoldingsUpdateOwnershipResponse.body(), containsString("not found"));
    assertThat(postHoldingsUpdateOwnershipResponse.body(), containsString(invalidInstanceId.toString()));
  }

  @Test
  void cannotUpdateHoldingsRecordOwnershipOfNonSharedInstance() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstance(instanceId);

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())),
      UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.statusCode(), is(400));

    assertThat(postHoldingsUpdateOwnershipResponse.body(),
      containsString(String.format("Instance with id: %s is not shared", instanceId)));
  }

  @Test
  @SneakyThrows
  void cannotUpdateHoldingsRecordOwnershipDueToHoldingsRecordCreateError() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    createSharedInstanceAcrossTenants(instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Server error");
    collegeHoldingsStorageClient.emulateFailure(500, HttpMethod.POST.name(), expectedErrorResponse.toString());

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString())), UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    collegeHoldingsStorageClient.disableFailureEmulation();

    assertThat(postHoldingsUpdateOwnershipResponse,
      hasNotUpdatedEntity(createHoldingsRecord1.toString(), expectedErrorResponse.toString()));

    assertThat(postHoldingsUpdateOwnershipResponse, hasStatusAndJsonBody(400));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    Response targetTenantHoldingsRecord1 = collegeHoldingsStorageClient.getById(createHoldingsRecord1);

    assertEquals(instanceId.toString(), sourceTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));
    assertEquals(HttpStatus.SC_NOT_FOUND, targetTenantHoldingsRecord1.statusCode());
  }

  @Test
  @SneakyThrows
  void cannotUpdateHoldingsRecordOwnershipDueToHoldingsRecordDeleteError() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    createSharedInstanceAcrossTenants(instance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Server error");
    collegeHoldingsStorageClient.emulateFailure(500, HttpMethod.DELETE.name(), expectedErrorResponse.toString());

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString())), UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    collegeHoldingsStorageClient.disableFailureEmulation();

    assertThat(postHoldingsUpdateOwnershipResponse,
      hasNotUpdatedEntity(createHoldingsRecord1.toString(), expectedErrorResponse.toString()));

    assertThat(postHoldingsUpdateOwnershipResponse, hasStatusAndJsonBody(400));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(1, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();

    assertEquals(instanceId.toString(), sourceTenantHoldingsRecord1.getJson().getString(INSTANCE_ID));
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));
  }

  @Test
  @SneakyThrows
  void canUpdateHoldingsRecordOwnershipToDifferentInstanceWithExtraRedundantFields() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    createSharedInstanceAcrossTenants(instance);

    JsonObject firstJsonHoldingsAsRequest = new HoldingRequestBuilder().forInstance(instanceId).create();
    final UUID createHoldingsRecord1 = holdingsStorageClient.create(
        HoldingsApiMoveTest.withExtraRedundantFields(firstJsonHoldingsAsRequest))
      .getId();

    JsonObject secondJsonHoldingsAsRequest = new HoldingRequestBuilder().forInstance(instanceId).create();
    final UUID createHoldingsRecord2 = holdingsStorageClient.create(
        HoldingsApiMoveTest.withExtraRedundantFields(secondJsonHoldingsAsRequest))
      .getId();

    JsonObject holdingsRecordUpdateOwnershipRequestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(createHoldingsRecord1.toString(), createHoldingsRecord2.toString())),
      UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postHoldingsUpdateOwnershipResponse =
      updateHoldingsRecordsOwnership(holdingsRecordUpdateOwnershipRequestBody);

    assertThat(postHoldingsUpdateOwnershipResponse.statusCode(), is(HttpStatus.SC_OK));
    assertThat(new JsonObject(postHoldingsUpdateOwnershipResponse.body()).getJsonArray("notUpdatedEntities").size(),
      is(0));
    assertThat(postHoldingsUpdateOwnershipResponse.contentType(), containsString(
      HttpHeaderValues.APPLICATION_JSON.toString()));

    Response sourceTenantHoldingsRecord1 = holdingsStorageClient.getById(createHoldingsRecord1);
    List<JsonObject> targetTenantHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 100);
    assertEquals(2, targetTenantHoldings.size());

    JsonObject targetTenantHoldingsRecord1 = targetTenantHoldings.getFirst();

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord1.statusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord1.getString(INSTANCE_ID));

    Response sourceTenantHoldingsRecord2 = holdingsStorageClient.getById(createHoldingsRecord2);
    JsonObject targetTenantHoldingsRecord2 = targetTenantHoldings.get(1);

    assertEquals(HttpStatus.SC_NOT_FOUND, sourceTenantHoldingsRecord2.statusCode());
    assertEquals(instanceId.toString(), targetTenantHoldingsRecord2.getString(INSTANCE_ID));
  }

  @Test
  @SneakyThrows
  void canUpdateOwnershipOfMarcHoldingAndMoveSrsRecord() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

    final UUID holdingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    final JsonObject srsRecordToCreate = MarcSourceRecordFixture.buildMarcSourceRecord(holdingsId);
    final String sourceSrsId = srsRecordToCreate.getString("id");
    sourceRecordStorageClient.create(srsRecordToCreate);

    ensureCollegeTenantLocationExists(getMainLibraryLocation());

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(holdingsId.toString())), UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);

    assertThat(response.statusCode(), is(HttpStatus.SC_OK));
    assertThat(new JsonObject(response.body()).getJsonArray("notUpdatedEntities").size(), is(0));

    //check that holding removed from source tenant
    Response sourceHoldingsResponse = holdingsStorageClient.getById(holdingsId);
    assertThat(sourceHoldingsResponse.statusCode(), is(HttpStatus.SC_NOT_FOUND));

    //check that holding created in target tenant
    List<JsonObject> targetHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 1);
    assertThat(targetHoldings.size(), is(1));
    assertThat(targetHoldings.getFirst().getString("id"), is(holdingsId.toString()));

    //check that SRS record from source tenant marked as DELETED
    Response sourceSrsResponse = sourceRecordStorageClient.getById(UUID.fromString(sourceSrsId));
    assertThat(sourceSrsResponse.statusCode(), is(HttpStatus.SC_NOT_FOUND));

    //check that SRS record created in target tenant
    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("matchedId==" + sourceSrsId, 1);
    assertThat(targetSrsRecords.size(), is(1));

    JsonObject targetSrsRecord = targetSrsRecords.getFirst();
    assertNotEquals(sourceSrsId, targetSrsRecord.getString("id"));
    assertEquals(sourceSrsId, targetSrsRecord.getString("matchedId"));
    assertEquals("MARC_HOLDING", targetSrsRecord.getString("recordType"));

    JsonObject parsedRecord = targetSrsRecord.getJsonObject("parsedRecord");
    JsonObject parsedContent = MarcSourceRecordFixture.getParsedContent(parsedRecord);

    List<String> field852bValues = MarcSourceRecordFixture.getField852bValues(parsedContent);
    assertEquals(1, field852bValues.size());
    assertEquals(MAIN_LIBRARY_LOCATION_CODE, field852bValues.getFirst());
    assertNotEquals("OLD_LOCATION_CODE", field852bValues.getFirst());
  }

  @Test
  @SneakyThrows
  void shouldRemoveExisting852bValueRegardlessOfIndicatorsWhenPopulatingLocationCode() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

    final UUID holdingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    // Source 852 field carries subfield 'b' under non-blank indicators (e.g. a different cataloging convention).
    final JsonObject srsRecordToCreate = MarcSourceRecordFixture.buildMarcSourceRecord(holdingsId, "4", "0");
    final String sourceSrsId = srsRecordToCreate.getString("id");
    sourceRecordStorageClient.create(srsRecordToCreate);

    ensureCollegeTenantLocationExists(getMainLibraryLocation());

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(holdingsId.toString())), UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);

    assertThat(response.statusCode(), is(HttpStatus.SC_OK));
    assertThat(response.getJson().getJsonArray("notUpdatedEntities").size(), is(0));

    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("matchedId==" + sourceSrsId, 1);
    assertThat(targetSrsRecords.size(), is(1));

    JsonObject parsedRecord = targetSrsRecords.getFirst().getJsonObject("parsedRecord");
    JsonObject parsedContent = MarcSourceRecordFixture.getParsedContent(parsedRecord);
    List<String> field852bValues = MarcSourceRecordFixture.getField852bValues(parsedContent);

    // The stale 852$b under non-blank indicators should be removed, leaving only the freshly populated one.
    assertEquals(1, field852bValues.size());
    assertEquals(MAIN_LIBRARY_LOCATION_CODE, field852bValues.getFirst());
    assertNotEquals("OLD_LOCATION_CODE", field852bValues.getFirst());
  }

  @Test
  @SneakyThrows
  void shouldFailMarcHoldingsAndMoveFolioHoldingWhenSnapshotCreationFails() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

    final UUID marcHoldingsId1 = holdingsStorageClient.create(
        new HoldingRequestBuilder().forInstance(instanceId).withMarcSource())
      .getId();
    final UUID marcHoldingsId2 = holdingsStorageClient.create(
        new HoldingRequestBuilder().forInstance(instanceId).withMarcSource())
      .getId();
    final UUID folioHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder().forInstance(instanceId))
      .getId();

    sourceRecordStorageClient.create(MarcSourceRecordFixture.buildMarcSourceRecord(marcHoldingsId1));
    sourceRecordStorageClient.create(MarcSourceRecordFixture.buildMarcSourceRecord(marcHoldingsId2));

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
      response.statusCode(), is(HttpStatus.SC_BAD_REQUEST));

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
      holdingsStorageClient.getById(folioHoldingsId).statusCode(), is(HttpStatus.SC_NOT_FOUND));
    List<JsonObject> targetFolioHoldings = collegeHoldingsStorageClient
      .getMany(String.format("id==%s", folioHoldingsId), 1);
    assertThat("FOLIO holding should be created in target tenant", targetFolioHoldings.size(), is(1));

    //Check that MARC holdings still exist in source tenant
    assertThat("MARC holding 1 should still exist in source tenant",
      holdingsStorageClient.getById(marcHoldingsId1).statusCode(), is(HttpStatus.SC_OK));
    assertThat("MARC holding 2 should still exist in source tenant",
      holdingsStorageClient.getById(marcHoldingsId2).statusCode(), is(HttpStatus.SC_OK));

    //Check that MARC holdings created in target tenant
    List<JsonObject> targetMarcHoldings = collegeHoldingsStorageClient
      .getMany(String.format("id==(%s or %s)", marcHoldingsId1, marcHoldingsId2), 2);
    assertThat("MARC holdings are created in target tenant", targetMarcHoldings.size(), is(2));

    // Check that no SRS records created in target tenant
    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("recordType=MARC_HOLDING", 2);
    assertThat("No SRS records should be created in target tenant", targetSrsRecords.size(), is(0));
  }

  @Test
  @SneakyThrows
  void shouldFailOneMarcAndMoveOtherHoldingsWhenSingleSrsRecordMoveFails() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

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
      MarcSourceRecordFixture.buildMarcSourceRecord(successfulMarcId)).getJson().getString("id");

    sourceRecordStorageClient.create(MarcSourceRecordFixture.buildMarcSourceRecord(failingMarcId));

    final JsonObject expectedErrorResponse =
      new JsonObject().put("message", "Internal Server Error: Record creation failed");
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
      response.statusCode(), is(HttpStatus.SC_BAD_REQUEST));

    JsonObject responseBody = response.getJson();
    JsonArray notUpdatedEntities = responseBody.getJsonArray("notUpdatedEntities");
    assertThat("Should have exactly one not-updated entity", notUpdatedEntities.size(), is(1));
    assertThat("Failed entity should be the failing MARC holding",
      notUpdatedEntities.getJsonObject(0).getString("entityId"), is(failingMarcId.toString()));

    // Check that FOLIO holding was moved to target tenant
    assertThat("FOLIO holding should be deleted from source tenant",
      holdingsStorageClient.getById(folioHoldingsId).statusCode(), is(HttpStatus.SC_NOT_FOUND));
    assertThat("FOLIO holding should be created in target tenant",
      collegeHoldingsStorageClient.getMany(String.format("id==%s", folioHoldingsId), 1).size(), is(1));

    // Check that successful MARC holding was moved to target tenant
    assertThat("Successful MARC holding should be deleted from source tenant",
      holdingsStorageClient.getById(successfulMarcId).statusCode(), is(HttpStatus.SC_NOT_FOUND));
    assertThat("Successful MARC holding should be created in target tenant",
      collegeHoldingsStorageClient.getMany(String.format("id==%s", successfulMarcId), 1).size(), is(1));
    Response sourceSrsForSuccess = sourceRecordStorageClient.getById(UUID.fromString(successfulSrsId));
    assertThat("Source SRS for successful holding should be marked as DELETED",
      sourceSrsForSuccess.statusCode(), is(HttpStatus.SC_NOT_FOUND));

    // Check that failing MARC holding still exists in source tenant
    assertThat("Failing MARC holding should still exist in source tenant",
      holdingsStorageClient.getById(failingMarcId).statusCode(), is(HttpStatus.SC_OK));
    assertThat("Failing MARC holding SHOULD BE created in target tenant despite SRS failure",
      collegeHoldingsStorageClient.getMany(String.format("id==%s", failingMarcId), 1).size(), is(1));
    assertThat("SRS record for failing MARC holding should NOT be created in target tenant",
      collegeSourceRecordStorageClient.getMany(String.format("externalIdsHolder.holdingsId==%s", failingMarcId), 1)
        .size(), is(0));

    // Check that one SRS record created in target tenant for successful holding
    // and it corresponds to the successful holding
    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("recordType==MARC_HOLDING", 2);
    assertThat("Exactly one SRS record should be created in target tenant", targetSrsRecords.size(), is(1));
    assertThat("The created SRS record should correspond to the successful holding",
      targetSrsRecords.getFirst().getString("matchedId"), is(successfulSrsId));
  }

  @Test
  @SneakyThrows
  void shouldReturn400AndNotUpdateMarcHoldingWhenSrsRecordIsNotFound() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

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
    assertThat(response.statusCode(), is(HttpStatus.SC_BAD_REQUEST));

    assertThat(response, hasNotUpdatedEntity(failingHoldingsId.toString(), "Failed to fetch MARC source record"));

    // marc holding without SRS record should not be moved to target tenant
    Response failingSourceHoldingsResponse = holdingsStorageClient.getById(failingHoldingsId);
    assertThat(failingSourceHoldingsResponse.statusCode(), is(HttpStatus.SC_OK));

    // FOLIO holding should br moved to target tenant
    Response successfulSourceHoldingsResponse = holdingsStorageClient.getById(successfulHoldingsId);
    assertThat(successfulSourceHoldingsResponse.statusCode(), is(HttpStatus.SC_NOT_FOUND));

    // check that one holding created in target tenant
    List<JsonObject> targetHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId=%s", instanceId), 1);
    assertThat(targetHoldings.size(), is(1));
  }

  @Test
  @SneakyThrows
  void shouldReturn400WhenMarcSrsRecordCreationFailsInTargetTenant() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

    final UUID marcHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource())
      .getId();

    final JsonObject sourceSrsRecord = MarcSourceRecordFixture.buildMarcSourceRecord(marcHoldingsId);
    final String sourceSrsId = sourceSrsRecord.getString("id");
    sourceRecordStorageClient.create(sourceSrsRecord);

    ensureCollegeTenantLocationExists(getMainLibraryLocation());

    collegeSourceRecordStorageClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(5).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(new JsonObject().put("message", "target MARC SRS create failed").toString())
        .setMethod(HttpMethod.POST.name())
        .setBodyContains(marcHoldingsId.toString())
    );

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingsId.toString())),
      UUID.fromString(getMainLibraryLocation()), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);
    collegeSourceRecordStorageClient.disableFailureEmulation();

    assertThat(response.statusCode(), is(HttpStatus.SC_BAD_REQUEST));

    assertThat(response,
      hasNotUpdatedEntity(marcHoldingsId.toString(), "Failed to post SRS record to target tenant=college"));

    // Holdings stays in source tenant when MARC SRS move cannot be completed.
    assertThat(holdingsStorageClient.getById(marcHoldingsId).statusCode(), is(HttpStatus.SC_OK));

    // Source SRS still exists and target SRS is not created.
    assertThat(sourceRecordStorageClient.getById(UUID.fromString(sourceSrsId)).statusCode(), is(HttpStatus.SC_OK));
    assertThat(collegeSourceRecordStorageClient.getMany("matchedId==" + sourceSrsId, 1).size(), is(0));
  }

  @Test
  @SneakyThrows
  void shouldDoNothingAndReportAllAsNotUpdatedWhenNoValidHoldingsRemainAfterValidation() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

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
    JsonObject boundWithPart =
      new BoundWithPartRequestBuilder(itemForBoundWith.getId().toString(), boundWithHoldingId.toString()).create();
    boundWithPartsStorageClient.create(boundWithPart);

    //ACTION
    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingId.toString(), boundWithHoldingId.toString())),
      UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);
    assertThat(response.statusCode(), is(HttpStatus.SC_BAD_REQUEST));

    // Should return 2 not-updated holdings
    JsonObject responseBody = response.getJson();
    JsonArray notUpdatedEntities = responseBody.getJsonArray("notUpdatedEntities");
    assertThat("There should be two not-updated entities", notUpdatedEntities.size(), is(2));

    List<JsonObject> errors = notUpdatedEntities.stream()
      .map(JsonObject.class::cast)
      .toList();

    // Check errors for MARC holding without SRS record
    assertTrue(errors.stream().anyMatch(error ->
      error.getString("entityId").equals(marcHoldingId.toString()) &&
      error.getString("errorMessage").contains("Failed to fetch MARC source record")
    ), "Should contain error for MARC holding without SRS");

    // Check errors for bound-with holding
    assertTrue(errors.stream().anyMatch(error ->
      error.getString("entityId").equals(boundWithHoldingId.toString()) &&
      error.getString("errorMessage").equals(String.format(HOLDING_BOUND_WITH_PARTS_ERROR, boundWithHoldingId))
    ), "Should contain error for bound-with holding");

    // Check that no holdings were moved to target tenant
    assertThat(holdingsStorageClient.getById(marcHoldingId).statusCode(), is(HttpStatus.SC_OK));
    assertThat(holdingsStorageClient.getById(boundWithHoldingId).statusCode(), is(HttpStatus.SC_OK));

    // Check that no holdings were created in target tenant
    List<JsonObject> targetHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId==%s", instanceId), 2);
    assertThat("No holdings should be created in the target tenant", targetHoldings.size(), is(0));
  }

  @Test
  @SneakyThrows
  void shouldReturn400AndReportErrorWhenSnapshotCreationFailsForMarcHolding() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

    // Create MARC holding which requires snapshot creation
    final UUID marcHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    // Create corresponding SRS record
    final JsonObject srsRecordToCreate = MarcSourceRecordFixture.buildMarcSourceRecord(marcHoldingsId);
    sourceRecordStorageClient.create(srsRecordToCreate);

    // Create regular FOLIO holding (no snapshot needed)
    final UUID folioHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
      )
      .getId();

    // Emulate failure on snapshot creation endpoint in target tenant (college)
    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Snapshot creation failed");
    collegeSourceRecordStorageClient.emulateFailure(500, HttpMethod.POST.name(), expectedErrorResponse.toString());

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingsId.toString(), folioHoldingsId.toString())),
      UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);

    collegeSourceRecordStorageClient.disableFailureEmulation();

    // Verify that the operation returns 400 due to snapshot creation failure
    assertThat(response.statusCode(), is(HttpStatus.SC_BAD_REQUEST));

    assertThat(response, hasNotUpdatedEntity(marcHoldingsId.toString(),
      "Failed to post SRS record to target tenant=college: {\"message\":\"Snapshot creation failed\"}"));

    // Verify MARC holding remains in source tenant due to SRS failure (partial failure)
    Response marcHoldingResponse = holdingsStorageClient.getById(marcHoldingsId);
    assertThat("MARC holding should still exist in source tenant due to SRS failure",
      marcHoldingResponse.statusCode(), is(HttpStatus.SC_OK));

    // Verify FOLIO holding was successfully moved to target tenant
    Response folioHoldingResponse = holdingsStorageClient.getById(folioHoldingsId);
    assertThat("FOLIO holding should be deleted from source tenant", folioHoldingResponse.statusCode(),
      is(HttpStatus.SC_NOT_FOUND));

    // Both holdings should be created in target tenant (holdings migration succeeds, SRS migration fails)
    List<JsonObject> targetHoldings =
      collegeHoldingsStorageClient.getMany(String.format("instanceId==%s", instanceId), 3);
    assertThat("Both holdings should be created in target tenant", targetHoldings.size(), is(2));

    List<String> targetHoldingIds = targetHoldings.stream().map(h -> h.getString("id")).toList();
    assertTrue(targetHoldingIds.contains(marcHoldingsId.toString()), "MARC holding should be in target tenant");
    assertTrue(targetHoldingIds.contains(folioHoldingsId.toString()), "FOLIO holding should be in target tenant");
  }

  @Test
  @SneakyThrows
  void shouldFallbackToLocationIdWhenFetchingTargetLocationFailsForMarcHolding() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

    final UUID marcHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    final JsonObject srsRecordToCreate = MarcSourceRecordFixture.buildMarcSourceRecord(marcHoldingsId);
    final String sourceSrsId = srsRecordToCreate.getString("id");
    sourceRecordStorageClient.create(srsRecordToCreate);

    String targetLocationId = getMainLibraryLocation();
    ensureCollegeTenantLocationExists(targetLocationId);

    ResourceClient collegeLocationsClient = ResourceClient.forLocations(collegeOkapiClient);
    collegeLocationsClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(5).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(new JsonObject().put("message", "Location service unavailable").toString())
        .setMethod(HttpMethod.GET.name())
        .setUrlPattern("/locations/.*")
    );

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingsId.toString())), UUID.fromString(targetLocationId),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);
    collegeLocationsClient.disableFailureEmulation();

    assertThat(response.statusCode(), is(HttpStatus.SC_OK));
    assertThat(response.getJson().getJsonArray("notUpdatedEntities").size(), is(0));

    // Operation should complete and use locationId as fallback when GET /locations fails.
    assertThat(holdingsStorageClient.getById(marcHoldingsId).statusCode(), is(HttpStatus.SC_NOT_FOUND));

    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("matchedId==" + sourceSrsId, 1);
    assertThat(targetSrsRecords.size(), is(1));

    JsonObject parsedRecord = targetSrsRecords.getFirst().getJsonObject("parsedRecord");
    JsonObject parsedContent = MarcSourceRecordFixture.getParsedContent(parsedRecord);
    List<String> field852bValues = MarcSourceRecordFixture.getField852bValues(parsedContent);

    assertThat(field852bValues.size(), is(1));
    assertThat(field852bValues.getFirst(), is(targetLocationId));
  }

  @Test
  @SneakyThrows
  void shouldFallbackToLocationIdWhenFetchedLocationCodeIsEmpty() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

    final UUID marcHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    final JsonObject srsRecordToCreate = MarcSourceRecordFixture.buildMarcSourceRecord(marcHoldingsId);
    final String sourceSrsId = srsRecordToCreate.getString("id");
    sourceRecordStorageClient.create(srsRecordToCreate);

    String targetLocationId = UUID.randomUUID().toString();

    ResourceClient collegeLocationsClient = ResourceClient.forLocations(collegeOkapiClient);
    collegeLocationsClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(5).toDate())
        .setStatusCode(200)
        .setContentType("application/json")
        .setBody(new JsonObject().put("id", targetLocationId).put("code", "").toString())
        .setMethod(HttpMethod.GET.name())
        .setUrlPattern("/locations/" + targetLocationId)
    );

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingsId.toString())), UUID.fromString(targetLocationId),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);
    collegeLocationsClient.disableFailureEmulation();

    assertThat(response.statusCode(), is(HttpStatus.SC_OK));
    assertThat(response.getJson().getJsonArray("notUpdatedEntities").size(), is(0));

    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("matchedId==" + sourceSrsId, 1);
    assertThat(targetSrsRecords.size(), is(1));

    JsonObject parsedRecord = targetSrsRecords.getFirst().getJsonObject("parsedRecord");
    JsonObject parsedContent = MarcSourceRecordFixture.getParsedContent(parsedRecord);
    List<String> field852bValues = MarcSourceRecordFixture.getField852bValues(parsedContent);

    assertThat(field852bValues.size(), is(1));
    assertThat(field852bValues.getFirst(), is(targetLocationId));
  }

  @Test
  @SneakyThrows
  void shouldFallbackToLocationIdWhenLocationResponseIsNotValidJson() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

    final UUID marcHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    final JsonObject srsRecordToCreate = MarcSourceRecordFixture.buildMarcSourceRecord(marcHoldingsId);
    final String sourceSrsId = srsRecordToCreate.getString("id");
    sourceRecordStorageClient.create(srsRecordToCreate);

    String targetLocationId = UUID.randomUUID().toString();

    ResourceClient collegeLocationsClient = ResourceClient.forLocations(collegeOkapiClient);
    collegeLocationsClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(5).toDate())
        .setStatusCode(200)
        .setContentType("application/json")
        .setBody("not-a-valid-json-body")
        .setMethod(HttpMethod.GET.name())
        .setUrlPattern("/locations/" + targetLocationId)
    );

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingsId.toString())), UUID.fromString(targetLocationId),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);
    collegeLocationsClient.disableFailureEmulation();

    assertThat(response.statusCode(), is(HttpStatus.SC_OK));
    assertThat(response.getJson().getJsonArray("notUpdatedEntities").size(), is(0));

    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("matchedId==" + sourceSrsId, 1);
    assertThat(targetSrsRecords.size(), is(1));

    JsonObject parsedRecord = targetSrsRecords.getFirst().getJsonObject("parsedRecord");
    JsonObject parsedContent = MarcSourceRecordFixture.getParsedContent(parsedRecord);
    List<String> field852bValues = MarcSourceRecordFixture.getField852bValues(parsedContent);

    // Operation should complete and use locationId as fallback when the location response body cannot be parsed.
    assertThat(field852bValues.size(), is(1));
    assertThat(field852bValues.getFirst(), is(targetLocationId));
  }

  @Test
  @SneakyThrows
  void shouldFallbackToLocationIdWhenLocationResponseHasEmptyBody() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

    final UUID marcHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    final JsonObject srsRecordToCreate = MarcSourceRecordFixture.buildMarcSourceRecord(marcHoldingsId);
    final String sourceSrsId = srsRecordToCreate.getString("id");
    sourceRecordStorageClient.create(srsRecordToCreate);

    String targetLocationId = UUID.randomUUID().toString();

    ResourceClient collegeLocationsClient = ResourceClient.forLocations(collegeOkapiClient);
    collegeLocationsClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(5).toDate())
        .setStatusCode(200)
        .setContentType("application/json")
        .setBody("")
        .setMethod(HttpMethod.GET.name())
        .setUrlPattern("/locations/" + targetLocationId)
    );

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingsId.toString())), UUID.fromString(targetLocationId),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);
    collegeLocationsClient.disableFailureEmulation();

    assertThat(response.statusCode(), is(HttpStatus.SC_OK));
    assertThat(response.getJson().getJsonArray("notUpdatedEntities").size(), is(0));

    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("matchedId==" + sourceSrsId, 1);
    assertThat(targetSrsRecords.size(), is(1));

    JsonObject parsedRecord = targetSrsRecords.getFirst().getJsonObject("parsedRecord");
    JsonObject parsedContent = MarcSourceRecordFixture.getParsedContent(parsedRecord);
    List<String> field852bValues = MarcSourceRecordFixture.getField852bValues(parsedContent);

    // Operation should complete and use locationId as fallback when the location response has no body at all.
    assertThat(field852bValues.size(), is(1));
    assertThat(field852bValues.getFirst(), is(targetLocationId));
  }

  @Test
  @SneakyThrows
  void shouldFallbackToLocationIdWhenLocationResponseHasNoCodeField() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

    final UUID marcHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    final JsonObject srsRecordToCreate = MarcSourceRecordFixture.buildMarcSourceRecord(marcHoldingsId);
    final String sourceSrsId = srsRecordToCreate.getString("id");
    sourceRecordStorageClient.create(srsRecordToCreate);

    String targetLocationId = UUID.randomUUID().toString();

    ResourceClient collegeLocationsClient = ResourceClient.forLocations(collegeOkapiClient);
    collegeLocationsClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(5).toDate())
        .setStatusCode(200)
        .setContentType("application/json")
        .setBody(new JsonObject().put("id", targetLocationId).toString())
        .setMethod(HttpMethod.GET.name())
        .setUrlPattern("/locations/" + targetLocationId)
    );

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingsId.toString())), UUID.fromString(targetLocationId),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);
    collegeLocationsClient.disableFailureEmulation();

    assertThat(response.statusCode(), is(HttpStatus.SC_OK));
    assertThat(response.getJson().getJsonArray("notUpdatedEntities").size(), is(0));

    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("matchedId==" + sourceSrsId, 1);
    assertThat(targetSrsRecords.size(), is(1));

    JsonObject parsedRecord = targetSrsRecords.getFirst().getJsonObject("parsedRecord");
    JsonObject parsedContent = MarcSourceRecordFixture.getParsedContent(parsedRecord);
    List<String> field852bValues = MarcSourceRecordFixture.getField852bValues(parsedContent);

    // Operation should complete and use locationId as fallback when the location response has no "code" field at all.
    assertThat(field852bValues.size(), is(1));
    assertThat(field852bValues.getFirst(), is(targetLocationId));
  }

  @Test
  @SneakyThrows
  void shouldMarkHoldingAsNotUpdatedWhenSourceSrsRecordDeleteFails() {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    createSharedInstanceAcrossTenants(instance);

    final UUID marcHoldingsId = holdingsStorageClient.create(
        new HoldingRequestBuilder()
          .forInstance(instanceId)
          .withMarcSource()
      )
      .getId();

    final JsonObject srsRecordToCreate = MarcSourceRecordFixture.buildMarcSourceRecord(marcHoldingsId);
    final String sourceSrsId = srsRecordToCreate.getString("id");
    sourceRecordStorageClient.create(srsRecordToCreate);

    final JsonObject expectedErrorResponse = new JsonObject().put("message", "Internal Server Error: Delete failed");
    sourceRecordStorageClient.emulateFailure(
      new EndpointFailureDescriptor()
        .setFailureExpireDate(DateTime.now().plusSeconds(5).toDate())
        .setStatusCode(500)
        .setContentType("application/json")
        .setBody(expectedErrorResponse.toString())
        .setMethod(HttpMethod.DELETE.name())
    );

    JsonObject requestBody = new HoldingsRecordUpdateOwnershipRequestBuilder(instanceId,
      new JsonArray(List.of(marcHoldingsId.toString())), UUID.fromString(getMainLibraryLocation()),
      ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response response = updateHoldingsRecordsOwnership(requestBody);
    sourceRecordStorageClient.disableFailureEmulation();

    assertThat(response.statusCode(), is(HttpStatus.SC_BAD_REQUEST));

    assertThat(response, hasNotUpdatedEntity(marcHoldingsId.toString(), expectedErrorResponse.toString()));

    // Source holding is kept since the migration did not fully complete.
    assertThat(holdingsStorageClient.getById(marcHoldingsId).statusCode(), is(HttpStatus.SC_OK));

    // The SRS record was already posted to the target tenant before the delete step failed.
    List<JsonObject> targetSrsRecords = collegeSourceRecordStorageClient.getMany("matchedId==" + sourceSrsId, 1);
    assertThat(targetSrsRecords.size(), is(1));

    // Source SRS record still exists because its deletion failed.
    assertThat(sourceRecordStorageClient.getById(UUID.fromString(sourceSrsId)).statusCode(), is(HttpStatus.SC_OK));
  }

  private void ensureCollegeTenantLocationExists(String locationId)
    throws ExecutionException, InterruptedException, TimeoutException {
    JsonObject locationBody = new JsonObject()
      .put("id", locationId)
      .put("name", "Main Library (college test fixture)")
      .put("code", MAIN_LIBRARY_LOCATION_CODE)
      .put("institutionId", UUID.randomUUID().toString())
      .put("campusId", UUID.randomUUID().toString())
      .put("libraryId", UUID.randomUUID().toString())
      .put("primaryServicePoint", UUID.randomUUID().toString());

    Response createLocationResponse = collegeOkapiClient
      .post(StorageInterfaceUrls.locationsStorageUrl(""), locationBody)
      .toCompletableFuture()
      .get(30, TimeUnit.SECONDS);

    // 201 means created; 422 means already exists in this test environment.
    assertTrue(createLocationResponse.statusCode() == HttpStatus.SC_CREATED
               || createLocationResponse.statusCode() == HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @SneakyThrows
  private Response updateHoldingsRecordsOwnership(JsonObject holdingsRecordUpdateOwnershipRequestBody) {
    return getOnCompletion(okapiClient.post(
      ApiRoot.updateHoldingsRecordsOwnership(), holdingsRecordUpdateOwnershipRequestBody), 30, TimeUnit.SECONDS);
  }

  private UUID createHoldingForInstance(UUID instanceId) {
    return holdingsStorageClient.create(new HoldingRequestBuilder().withHrId("hol0000001").forInstance(instanceId))
      .getId();
  }
}
