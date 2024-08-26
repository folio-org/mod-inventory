package api.items;

import static api.ApiTestSuite.COLLEGE_TENANT_ID;
import static api.ApiTestSuite.CONSORTIA_TENANT_ID;
import static api.support.InstanceSamples.smallAngryPlanet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.inventory.resources.TenantItems.ITEMS_FIELD;
import static org.folio.inventory.resources.TenantItems.TENANT_ITEMS_PATH;
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
import api.support.builders.ItemRequestBuilder;
import api.support.http.ResourceClient;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;

@RunWith(JUnitParamsRunner.class)
public class TenantItemApiTests extends ApiTests {

  @Test
  public void testTenantItemsGetFromDifferentTenants() throws MalformedURLException,
    ExecutionException, InterruptedException, TimeoutException {

    createConsortiumInstanceHoldingItem();
    createCollegeInstanceHoldingItem();

    var consortiumItem = getItems(consortiumOkapiClient, 1).get(0);
    var collegeItem = getItems(collegeOkapiClient, 1).get(0);

    var tenantItemPariCollection = constructTenantItemPairCollection(Map.of(
      CONSORTIA_TENANT_ID, consortiumItem.getString(ID),
      COLLEGE_TENANT_ID, collegeItem.getString(ID)
    ));
    var response = okapiClient.post(TENANT_ITEMS_PATH, JsonObject.mapFrom(tenantItemPariCollection))
      .toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThat(response.getStatusCode()).isEqualTo(200);
    var items = extractItems(response, 2);

    assertThat(items).contains(consortiumItem, collegeItem);
  }

  private void createConsortiumInstanceHoldingItem() {
    createInstanceHoldingItem(consortiumItemsClient, consortiumHoldingsStorageClient, consortiumOkapiClient);
  }

  private void createCollegeInstanceHoldingItem() {
    createInstanceHoldingItem(collegeItemsClient, collegeHoldingsStorageClient, collegeOkapiClient);
  }

  private void createInstanceHoldingItem(ResourceClient itemStorageClient, ResourceClient holdingsStorageClient, OkapiHttpClient okapiHttpClient) {
    var instanceId = UUID.randomUUID();
    InstanceApiClient.createInstance(okapiHttpClient, smallAngryPlanet(instanceId));
    var holdingId = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instanceId)).getId();
    itemStorageClient.create(new ItemRequestBuilder().forHolding(holdingId)
      .book().canCirculate().withBarcode(String.valueOf(Math.random() * 100)));
  }

  private List<JsonObject> getItems(OkapiHttpClient okapiHttpClient, int expected)
    throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException {

    var itemsResponse = okapiHttpClient.get(ApiRoot.items()).toCompletableFuture().get(5, SECONDS);
    assertThat(itemsResponse.getStatusCode()).isEqualTo(200);

    return extractItems(itemsResponse, expected);
  }

  private List<JsonObject> extractItems(Response itemsResponse, int expected) {
    var itemsCollection = itemsResponse.getJson();
    var items = JsonArrayHelper.toList(itemsCollection.getJsonArray(ITEMS_FIELD));
    assertThat(items).hasSize(expected);
    assertThat(itemsCollection.getInteger(TOTAL_RECORDS_FIELD)).isEqualTo(expected);
    return items;
  }

  private TenantItemPairCollection constructTenantItemPairCollection(Map<String, String> itemToTenantIds) {
    return new TenantItemPairCollection()
      .withItemTenantPairs(itemToTenantIds.entrySet().stream()
        .map(pair -> new TenantItemPair().withItemId(pair.getKey()).withTenantId(pair.getValue()))
        .toList());
  }

}
