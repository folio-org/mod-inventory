package api.items;

import api.ApiTestSuite;
import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import api.support.builders.ItemsUpdateOwnershipRequestBuilder;
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

import static api.ApiTestSuite.COLLEGE_TENANT_ID;
import static api.ApiTestSuite.TENANT_ID;
import static api.ApiTestSuite.createConsortiumTenant;
import static api.BoundWithTests.makeObjectBoundWithPart;
import static api.support.InstanceSamples.smallAngryPlanet;
import static java.lang.String.format;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;
import static org.folio.inventory.domain.items.Item.ORDER;
import static org.folio.inventory.resources.UpdateOwnershipApi.HOLDINGS_RECORD_NOT_FOUND;
import static org.folio.inventory.resources.UpdateOwnershipApi.INSTANCE_RELATED_TO_HOLDINGS_RECORD_NOT_SHARED;
import static org.folio.inventory.resources.UpdateOwnershipApi.ITEM_NOT_FOUND;
import static org.folio.inventory.resources.UpdateOwnershipApi.ITEM_NOT_LINKED_TO_SHARED_INSTANCE;
import static org.folio.inventory.resources.UpdateOwnershipApi.ITEM_WITH_PARTS_ERROR;
import static org.folio.inventory.support.ItemUtil.HOLDINGS_RECORD_ID;
import static org.folio.inventory.support.ItemUtil.PERMANENT_LOCATION_ID_KEY;
import static org.folio.inventory.support.ItemUtil.TEMPORARY_LOCATION_ID_KEY;
import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class ItemUpdateOwnershipApiTest extends ApiTests {

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
  public void canUpdateItemsOwnershipToDifferentTenant() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    String itemHrId = "it0000001";
    String locationId = UUID.randomUUID().toString();
    JsonObject location = new JsonObject().put("id", locationId).put("name", "location");

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));
    InstanceApiClient.createInstance(collegeOkapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstanceAtCollege(instanceId);

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
        .forHolding(createHoldingsRecord1)
        .withHrid(itemHrId)
        .withBarcode("645398607546")
        .withStatus(ItemStatusName.AVAILABLE.value())
        .withTemporaryLocation(location)
        .withPermanentLocation(location));

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(createHoldingsRecord2,
      new JsonArray(List.of(firstItem.getId().toString(), secondItem.getId().toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(toList(postItemsUpdateOwnershipResponse.getJson(), "notUpdatedEntities"), hasSize(0));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    final var sourceFirstUpdatedItem = itemsClient.getById(firstItem.getId());
    final var targetHoldingsRecordItems = collegeItemsClient.getMany(format("holdingsRecordId=%s", createHoldingsRecord2), 100);
    assertEquals(2, targetHoldingsRecordItems.size());

    JsonObject targetTenantItem1 = targetHoldingsRecordItems.get(0);
    JsonObject targetTenantItem2 = targetHoldingsRecordItems.get(1);

    var targetTenantItemsIds = targetHoldingsRecordItems.stream()
      .map(object -> object.getString(ID))
      .toList();

    final var sourceSecondUpdatedItem = itemsClient.getById(secondItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceFirstUpdatedItem.getStatusCode()));
    assertThat(targetTenantItem1.getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));
    assertTrue(targetTenantItemsIds.contains(firstItem.getId().toString()));
    assertTrue(targetTenantItemsIds.contains(secondItem.getId().toString()));

    var targetTenantHoldingsIds = targetHoldingsRecordItems.stream()
      .map(object -> object.getString(HOLDINGS_RECORD_ID))
      .toList();

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceSecondUpdatedItem.getStatusCode()));
    assertTrue(targetTenantHoldingsIds.contains(createHoldingsRecord2.toString()));
    assertNotEquals(targetTenantItem2.getString("hrid"), itemHrId);

    targetHoldingsRecordItems.forEach(item -> {
      assertNull(item.getInteger(ORDER));
      assertNull(item.getString(TEMPORARY_LOCATION_ID_KEY));
      assertNull(item.getString(PERMANENT_LOCATION_ID_KEY));
    });
  }

  @Test
  public void shouldReportErrorsWhenOnlySomeRequestedItemsOwnershipCouldNotBeUpdated() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));
    InstanceApiClient.createInstance(collegeOkapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstanceAtCollege(instanceId);

    final var item = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var nonExistentItemId = UUID.randomUUID();

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(createHoldingsRecord2,
      new JsonArray(List.of(item.getId().toString(), nonExistentItemId.toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(400));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonArray notUpdatedEntitiesIds = postItemsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notUpdatedEntitiesIds.size(), is(1));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("entityId"), equalTo(nonExistentItemId.toString()));
    assertEquals(format(ITEM_NOT_FOUND, nonExistentItemId, TENANT_ID), notUpdatedEntitiesIds.getJsonObject(0).getString("errorMessage"));

    final var sourceUpdatedItem = itemsClient.getById(item.getId());
    final var targetHoldingsRecordItems = collegeItemsClient.getMany(format("holdingsRecordId=%s", createHoldingsRecord2), 100);
    assertEquals(1, targetHoldingsRecordItems.size());

    JsonObject targetTenantItem1 = targetHoldingsRecordItems.get(0);

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceUpdatedItem.getStatusCode()));
    assertThat(targetTenantItem1.getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));
  }

  @Test
  public void shouldReportErrorWhenOnlySomeRequestedItemIsBoundWith() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);
    String itemHrId = "it0000001";

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));
    InstanceApiClient.createInstance(collegeOkapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstanceAtCollege(instanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var secondItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withHrid(itemHrId)
        .withBarcode("645398607546")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    JsonObject boundWithPart = makeObjectBoundWithPart(firstItem.getJson().getString("id"), createHoldingsRecord1.toString());
    boundWithPartsStorageClient.create(boundWithPart);

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(createHoldingsRecord2,
      new JsonArray(List.of(firstItem.getId().toString(), secondItem.getId().toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(400));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    JsonArray notFoundIds = postItemsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notFoundIds.size(), is(1));
    assertThat(notFoundIds.getJsonObject(0).getString("entityId"), equalTo(firstItem.getJson().getString("id")));
    assertThat(notFoundIds.getJsonObject(0).getString("errorMessage"),
      equalTo(String.format(ITEM_WITH_PARTS_ERROR, firstItem.getJson().getString("id"))));

    final var sourceFirstUpdatedItem = itemsClient.getById(firstItem.getId());
    final var targetHoldingsRecordItems = collegeItemsClient.getMany(format("holdingsRecordId=%s", createHoldingsRecord2), 100);
    assertEquals(1, targetHoldingsRecordItems.size());

    JsonObject targetTenantItem = targetHoldingsRecordItems.get(0);

    final var sourceSecondUpdatedItem = itemsClient.getById(secondItem.getId());

    assertThat(HttpStatus.SC_OK, is(sourceFirstUpdatedItem.getStatusCode()));

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceSecondUpdatedItem.getStatusCode()));
    assertThat(targetTenantItem.getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));
    assertEquals(secondItem.getId().toString(), targetTenantItem.getString(ID));
    assertNotEquals(targetTenantItem.getString("hrid"), itemHrId);
  }

  @Test
  public void shouldReportErrorWhenErrorDeletingItems() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));
    InstanceApiClient.createInstance(collegeOkapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstanceAtCollege(instanceId);

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

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(createHoldingsRecord2,
      new JsonArray(List.of(firstItem.getId().toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    collegeItemsClient.disableFailureEmulation();

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(400));
    JsonArray notUpdatedEntitiesIds = postItemsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notUpdatedEntitiesIds.size(), is(1));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("entityId"), equalTo(firstItem.getId().toString()));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("errorMessage"), containsString(expectedErrorResponse.toString()));

    final var sourceFirstUpdatedItem = itemsClient.getById(firstItem.getId());
    final var targetHoldingsRecordItems = collegeItemsClient.getMany(format("holdingsRecordId=%s", createHoldingsRecord2), 100);
    assertEquals(1, targetHoldingsRecordItems.size());

    JsonObject targetTenantItem1 = targetHoldingsRecordItems.get(0);


    assertThat(HttpStatus.SC_OK, is(sourceFirstUpdatedItem.getStatusCode()));
    assertThat(targetTenantItem1.getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));
  }

  @Test
  public void shouldReportErrorWhenErrorCreatingItems() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));
    InstanceApiClient.createInstance(collegeOkapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstanceAtCollege(instanceId);

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

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(createHoldingsRecord2,
      new JsonArray(List.of(firstItem.getId().toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    collegeItemsClient.disableFailureEmulation();

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(400));
    JsonArray notUpdatedEntitiesIds = postItemsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notUpdatedEntitiesIds.size(), is(1));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("entityId"), equalTo(firstItem.getId().toString()));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("errorMessage"), containsString(expectedErrorResponse.toString()));

    final var sourceFirstUpdatedItem = itemsClient.getById(firstItem.getId());
    final var targetHoldingsRecordItems = collegeItemsClient.getMany(format("holdingsRecordId=%s", createHoldingsRecord2), 100);
    assertEquals(0, targetHoldingsRecordItems.size());

    assertThat(HttpStatus.SC_OK, is(sourceFirstUpdatedItem.getStatusCode()));
  }

  @Test
  public void cannotUpdateItemsOwnershipToUnspecifiedHoldingsRecord() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(null,
      new JsonArray(List.of(UUID.randomUUID().toString())), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("errors"));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("toHoldingsRecordId"));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("toHoldingsRecordId is a required field"));
  }

  @Test
  public void cannotUpdateOwnershipOfUnspecifiedItems() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(UUID.randomUUID(),
      new JsonArray(List.of()), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("errors"));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("itemIds"));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("itemIds is a required field"));
  }

  @Test
  public void cannotUpdateItemsOwnershipToUnspecifiedTenant() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(UUID.randomUUID(),
      new JsonArray(List.of(UUID.randomUUID())), null).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("errors"));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("targetTenantId"));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("targetTenantId is a required field"));
  }

  @Test
  public void cannotUpdateItemsOwnershipToNonExistedHoldingsRecord() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));
    InstanceApiClient.createInstance(collegeOkapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));

    final UUID existingHoldingsId = createHoldingForInstance(instanceId);
    final UUID nonExistentHoldingsId = UUID.randomUUID();

    final var item = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existingHoldingsId)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(nonExistentHoldingsId,
      new JsonArray(List.of(item.getId().toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(404));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), equalTo(format(HOLDINGS_RECORD_NOT_FOUND, nonExistentHoldingsId, COLLEGE_TENANT_ID)));
  }

  @Test
  public void cannotUpdateItemsOwnershipIfTenantNotInConsortium() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    userTenantsClient.deleteAll();

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(UUID.randomUUID(),
      new JsonArray(List.of(UUID.randomUUID().toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(400));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("tenant is not in consortia"));
    createConsortiumTenant();
  }

  @Test
  public void cannotUpdateItemsOwnershipIfToHoldingsRecordIdNotLinkedToSharedInstance() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));
    InstanceApiClient.createInstance(collegeOkapiClient, instance.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord2 = createHoldingForInstanceAtCollege(instanceId);

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(createHoldingsRecord2,
      new JsonArray(List.of(UUID.randomUUID().toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(400));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), equalTo(String.format(INSTANCE_RELATED_TO_HOLDINGS_RECORD_NOT_SHARED, instanceId, createHoldingsRecord2)));
  }

  @Test
  public void shouldNotUpdateOwnershipOfItemsLinkedToAnotherInstance() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    UUID instanceId2 = UUID.randomUUID();
    JsonObject instance2 = smallAngryPlanet(instanceId2);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));
    InstanceApiClient.createInstance(collegeOkapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));

    InstanceApiClient.createInstance(okapiClient, instance2.put("source", FOLIO.getValue()));

    final UUID createHoldingsRecord1 = createHoldingForInstance(instanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstanceAtCollege(instanceId);

    final UUID createHoldingsRecord3 = createHoldingForInstance(instanceId2);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var secondItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord3)
        .withBarcode("645398607546")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(createHoldingsRecord2,
      new JsonArray(List.of(firstItem.getId().toString(), secondItem.getId().toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(400));

    JsonArray notUpdatedEntitiesIds = postItemsUpdateOwnershipResponse.getJson()
      .getJsonArray("notUpdatedEntities");

    assertThat(notUpdatedEntitiesIds.size(), is(1));
    assertThat(notUpdatedEntitiesIds.getJsonObject(0).getString("entityId"), equalTo(secondItem.getId().toString()));
    assertEquals(format(ITEM_NOT_LINKED_TO_SHARED_INSTANCE, secondItem.getId().toString()), notUpdatedEntitiesIds.getJsonObject(0).getString("errorMessage"));

    final var sourceFirstUpdatedItem = itemsClient.getById(firstItem.getId());
    final var targetHoldingsRecordItems = collegeItemsClient.getMany(format("holdingsRecordId=%s", createHoldingsRecord2), 100);
    assertEquals(1, targetHoldingsRecordItems.size());

    JsonObject targetTenantItem1 = targetHoldingsRecordItems.get(0);

    final var sourceSecondUpdatedItem = itemsClient.getById(secondItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceFirstUpdatedItem.getStatusCode()));
    assertThat(targetTenantItem1.getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));

    assertThat(HttpStatus.SC_OK, is(sourceSecondUpdatedItem.getStatusCode()));
  }

  private Response updateItemsOwnership(JsonObject itemsUpdateOwnershipRequestBody) throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    final var postItemsUpdateOwnershipCompleted = okapiClient.post(
      ApiRoot.updateItemsOwnership(), itemsUpdateOwnershipRequestBody);
    return postItemsUpdateOwnershipCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private UUID createHoldingForInstance(UUID instanceId) {
    return holdingsStorageClient.create(new HoldingRequestBuilder().forInstance(instanceId))
      .getId();
  }

  private UUID createHoldingForInstanceAtCollege(UUID instanceId) {
    return collegeHoldingsStorageClient.create(new HoldingRequestBuilder().forInstance(instanceId))
      .getId();
  }
}
