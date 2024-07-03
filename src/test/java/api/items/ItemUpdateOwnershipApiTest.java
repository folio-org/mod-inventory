package api.items;

import api.ApiTestSuite;
import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import api.support.builders.ItemsUpdateOwnershipRequestBuilder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import org.apache.http.HttpStatus;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.support.http.client.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.MalformedURLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static api.ApiTestSuite.COLLEGE_TENANT_ID;
import static api.ApiTestSuite.TENANT_ID;
import static api.ApiTestSuite.createConsortiumTenant;
import static api.support.InstanceSamples.smallAngryPlanet;
import static java.lang.String.format;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;
import static org.folio.inventory.resources.UpdateOwnershipApi.HOLDINGS_RECORD_NOT_FOUND;
import static org.folio.inventory.resources.UpdateOwnershipApi.ITEM_NOT_FOUND;
import static org.folio.inventory.support.ItemUtil.HOLDINGS_RECORD_ID;
import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class ItemUpdateOwnershipApiTest extends ApiTests {

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

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));
    InstanceApiClient.createInstance(collegeOkapiClient, instance.put("source", FOLIO.getValue()));

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
        .withBarcode("645398607546")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(createHoldingsRecord2,
      new JsonArray(List.of(firstItem.getId().toString(), secondItem.getId().toString())), ApiTestSuite.COLLEGE_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(toList(postItemsUpdateOwnershipResponse.getJson(), "nonUpdatedIds"), hasSize(0));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    final var sourceFirstUpdatedItem = itemsClient.getById(firstItem.getId());
    final var targetHoldingsRecordItems = collegeItemsClient.getMany(format("holdingsRecordId=%s", createHoldingsRecord2), 100);
    assertEquals(2, targetHoldingsRecordItems.size());

    JsonObject targetTenantItem1 = targetHoldingsRecordItems.get(0);
    JsonObject targetTenantItem2 = targetHoldingsRecordItems.get(1);

    final var sourceSecondUpdatedItem = itemsClient.getById(secondItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceFirstUpdatedItem.getStatusCode()));
    assertThat(targetTenantItem1.getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceSecondUpdatedItem.getStatusCode()));
    assertThat(targetTenantItem2.getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));
  }

  @Test
  public void shouldReportErrorsWhenOnlySomeRequestedItemsOwnershipCouldNotBeUpdated() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID instanceId = UUID.randomUUID();
    JsonObject instance = smallAngryPlanet(instanceId);

    InstanceApiClient.createInstance(okapiClient, instance.put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.put("source", FOLIO.getValue()));
    InstanceApiClient.createInstance(collegeOkapiClient, instance.put("source", FOLIO.getValue()));

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

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(200));
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
    InstanceApiClient.createInstance(collegeOkapiClient, instance.put("source", FOLIO.getValue()));

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
