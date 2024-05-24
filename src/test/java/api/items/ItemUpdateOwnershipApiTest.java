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
import static org.folio.inventory.support.ItemUtil.HOLDINGS_RECORD_ID;
import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@Ignore
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
    UUID sourceInstanceId = UUID.randomUUID();
    JsonObject sourceInstance = smallAngryPlanet(sourceInstanceId);

    UUID targetInstanceId = UUID.randomUUID();
    JsonObject targetInstance = smallAngryPlanet(targetInstanceId);

    InstanceApiClient.createInstance(okapiClient, sourceInstance);
    InstanceApiClient.createInstance(consortiumOkapiClient, targetInstance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(sourceInstanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstanceAtConsortium(targetInstanceId);

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
      new JsonArray(List.of(firstItem.getId().toString(), secondItem.getId().toString())), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(toList(postItemsUpdateOwnershipResponse.getJson(), "nonUpdatedIds"), hasSize(0));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    final var sourceFirstUpdatedItem = itemsClient.getById(firstItem.getId());
    final var targetFirstUpdatedItem = consortiumItemsClient.getById(firstItem.getId());

    final var sourceSecondUpdatedItem = itemsClient.getById(secondItem.getId());
    final var targetSecondUpdatedItem = consortiumItemsClient.getById(secondItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceFirstUpdatedItem.getStatusCode()));
    assertThat(targetFirstUpdatedItem.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceSecondUpdatedItem.getStatusCode()));
    assertThat(targetSecondUpdatedItem.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));
  }

  @Test
  public void shouldReportErrorsWhenOnlySomeRequestedItemsOwnershipCouldNotBeUpdated() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID sourceInstanceId = UUID.randomUUID();
    JsonObject sourceInstance = smallAngryPlanet(sourceInstanceId);

    UUID targetInstanceId = UUID.randomUUID();
    JsonObject targetInstance = smallAngryPlanet(targetInstanceId);

    InstanceApiClient.createInstance(okapiClient, sourceInstance);
    InstanceApiClient.createInstance(consortiumOkapiClient, targetInstance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(sourceInstanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstanceAtConsortium(targetInstanceId);

    final var item = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var nonExistentItemId = UUID.randomUUID();

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(createHoldingsRecord2,
      new JsonArray(List.of(item.getId().toString(), nonExistentItemId.toString())), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    final var notFoundIds = toListOfStrings(postItemsUpdateOwnershipResponse.getJson(),
      "nonUpdatedIds");

    assertThat(notFoundIds, hasSize(1));
    assertThat(notFoundIds.get(0), equalTo(nonExistentItemId.toString()));

    final var sourceUpdatedItem = itemsClient.getById(item.getId());
    final var targetUpdatedItem = consortiumItemsClient.getById(item.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceUpdatedItem.getStatusCode()));
    assertThat(targetUpdatedItem.getJson().getString(HOLDINGS_RECORD_ID), is(createHoldingsRecord2.toString()));
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
    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("Item ids aren't specified"));
  }

  @Test
  public void cannotUpdateItemsOwnershipToUnspecifiedTenant() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(UUID.randomUUID(),
      new JsonArray(List.of(UUID.randomUUID())), null).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("errors"));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("tenantId"));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("tenantId is a required field"));
  }

  @Test
  public void cannotUpdateItemsOwnershipToNonExistedHoldingsRecord() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID sourceInstanceId = UUID.randomUUID();
    JsonObject sourceInstance = smallAngryPlanet(sourceInstanceId);

    InstanceApiClient.createInstance(okapiClient, sourceInstance);

    final UUID existingHoldingsId = createHoldingForInstance(sourceInstanceId);
    final UUID nonExistentHoldingsId = UUID.randomUUID();

    final var item = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(existingHoldingsId)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(nonExistentHoldingsId,
      new JsonArray(List.of(item.getId().toString())), ApiTestSuite.CONSORTIA_TENANT_ID).create();

    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(422));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString("errors"));
    assertThat(postItemsUpdateOwnershipResponse.getBody(), containsString(nonExistentHoldingsId.toString()));
  }

  @Test
  public void canUpdateItemsOwnershipDueToItemUpdateError() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {
    UUID sourceInstanceId = UUID.randomUUID();
    JsonObject sourceInstance = smallAngryPlanet(sourceInstanceId);

    UUID targetInstanceId = UUID.randomUUID();
    JsonObject targetInstance = smallAngryPlanet(targetInstanceId);

    InstanceApiClient.createInstance(okapiClient, sourceInstance);
    InstanceApiClient.createInstance(consortiumOkapiClient, targetInstance);

    final UUID createHoldingsRecord1 = createHoldingForInstance(sourceInstanceId);
    final UUID createHoldingsRecord2 = createHoldingForInstanceAtConsortium(targetInstanceId);

    final var firstItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607547")
        .withStatus(ItemStatusName.AVAILABLE.value()));

    final var secondItem = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(createHoldingsRecord1)
        .withBarcode("645398607546")
        .withId(ID_FOR_FAILURE)
        .withStatus(ItemStatusName.AVAILABLE.value()));

    JsonObject itemsUpdateOwnershipRequestBody = new ItemsUpdateOwnershipRequestBuilder(createHoldingsRecord2,
      new JsonArray(List.of(firstItem.getId().toString(), secondItem.getId().toString())), ApiTestSuite.CONSORTIA_TENANT_ID).create();
    Response postItemsUpdateOwnershipResponse = updateItemsOwnership(itemsUpdateOwnershipRequestBody);

    final var nonUpdatedIdsIds = toListOfStrings(postItemsUpdateOwnershipResponse.getJson(),
      "nonUpdatedIds");

    assertThat(nonUpdatedIdsIds, hasSize(1));
    assertThat(nonUpdatedIdsIds.get(0), equalTo(ID_FOR_FAILURE.toString()));

    assertThat(postItemsUpdateOwnershipResponse.getStatusCode(), is(200));
    assertThat(postItemsUpdateOwnershipResponse.getContentType(), containsString(APPLICATION_JSON));

    final var sourceFirstItemUpdated = itemsClient.getById(firstItem.getId());
    final var targetFirstItemUpdated = consortiumItemsClient.getById(firstItem.getId());

    assertThat(HttpStatus.SC_NOT_FOUND, is(sourceFirstItemUpdated.getStatusCode()));
    assertThat(createHoldingsRecord2.toString(), equalTo(targetFirstItemUpdated.getJson().getString(HOLDINGS_RECORD_ID)));

    final var sourceSecondUpdatedItem = itemsClient.getById(secondItem.getId());
    final var targetSecondUpdatedItem = consortiumItemsClient.getById(secondItem.getId());

    assertThat(createHoldingsRecord1.toString(), equalTo(sourceSecondUpdatedItem.getJson().getString(HOLDINGS_RECORD_ID)));
    assertThat(HttpStatus.SC_NOT_FOUND, is(targetSecondUpdatedItem.getStatusCode()));
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

  private UUID createHoldingForInstanceAtConsortium(UUID instanceId) {
    return consortiumHoldingsStorageClient.create(new HoldingRequestBuilder().forInstance(instanceId))
      .getId();
  }
}
