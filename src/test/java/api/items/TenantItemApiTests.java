package api.items;

import static api.ApiTestSuite.COLLEGE_TENANT_ID;
import static api.ApiTestSuite.CONSORTIA_TENANT_ID;
import static api.ApiTestSuite.getBookMaterialType;
import static api.ApiTestSuite.getCanCirculateLoanType;
import static api.support.InstanceSamples.smallAngryPlanet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.inventory.resources.TenantItems.ITEMS_FIELD;
import static org.folio.inventory.resources.TenantItems.TOTAL_RECORDS_FIELD;
import static org.folio.inventory.support.ItemUtil.ID;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.TenantItemPair;
import org.folio.TenantItemPairCollection;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;
import org.junit.runner.RunWith;

import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.builders.HoldingRequestBuilder;
import api.support.http.ResourceClient;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;

@RunWith(JUnitParamsRunner.class)
public class TenantItemApiTests extends ApiTests {

  @Test
  public void testTenantItemsGetFromDifferentTenants() throws MalformedURLException,
    ExecutionException, InterruptedException, TimeoutException {

    var consortiumItemId = createConsortiumInstanceHoldingItem();
    var collegeItemId = createCollegeInstanceHoldingItem();
    var consortiumItem = consortiumItemsClient.getById(consortiumItemId).getJson();
    var collegeItem = collegeItemsClient.getById(collegeItemId).getJson();

    assertThat(consortiumItem.getString(ID)).matches(consortiumItemId.toString());
    assertThat(collegeItem.getString(ID)).matches(collegeItemId.toString());

    var tenantItemPairCollection = constructTenantItemPairCollection(Map.of(
      CONSORTIA_TENANT_ID, consortiumItem.getString(ID),
      COLLEGE_TENANT_ID, collegeItem.getString(ID)
    ));

    var response = okapiClient.post(ApiRoot.tenantItems(), JsonObject.mapFrom(tenantItemPairCollection))
      .toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThat(response.getStatusCode()).isEqualTo(200);

    var items = extractItems(response, 2);
    assertThat(items).contains(consortiumItem, collegeItem);
  }

  private UUID createConsortiumInstanceHoldingItem() {
    return createInstanceHoldingItem(consortiumItemsClient, consortiumHoldingsStorageClient, consortiumOkapiClient);
  }

  private UUID createCollegeInstanceHoldingItem() {
    return createInstanceHoldingItem(collegeItemsClient, collegeHoldingsStorageClient, collegeOkapiClient);
  }

  private UUID createInstanceHoldingItem(ResourceClient itemsStorageClient, ResourceClient holdingsStorageClient, OkapiHttpClient okapiHttpClient) {
    var instanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiHttpClient, smallAngryPlanet(instanceId));
    var holdingId = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instanceId)).getId();
    var itemId = UUID.randomUUID();
    var newItemRequest = JsonObject.of(
      "id", itemId.toString(),
      "status", new JsonObject().put("name", "Available"),
      "holdingsRecordId", holdingId,
      "materialTypeId", getBookMaterialType(),
      "permanentLoanTypeId", getCanCirculateLoanType());
    itemsStorageClient.create(newItemRequest);
    return itemId;
  }

  private List<JsonObject> extractItems(Response itemsResponse, int expected) {
    var itemsCollection = itemsResponse.getJson();
    var items = JsonArrayHelper.toList(itemsCollection.getJsonArray(ITEMS_FIELD));
    assertThat(items).hasSize(expected);
    assertThat(itemsCollection.getInteger(TOTAL_RECORDS_FIELD)).isEqualTo(expected);
    return items;
  }

  private TenantItemPairCollection constructTenantItemPairCollection(Map<String, String> tenantsToItemIds) {
    return new TenantItemPairCollection()
      .withTenantItemPairs(tenantsToItemIds.entrySet().stream()
        .map(pair -> new TenantItemPair().withTenantId(pair.getKey()).withItemId(pair.getValue()))
        .toList());
  }

}
