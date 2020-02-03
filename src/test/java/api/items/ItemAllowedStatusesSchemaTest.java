package api.items;

import static api.support.InstanceSamples.smallAngryPlanet;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.support.http.client.IndividualResource;
import org.junit.Test;
import org.junit.runner.RunWith;

import api.support.ApiTests;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * This test verifies that ramls/item.json and {@link ItemStatusName} is consistent.
 * <p>
 * If you're introducing a new status for an item you have to update both item.json
 * and the {@link ItemStatusName} enum, otherwise item won't pass validation
 * and will be rejected.
 */
@RunWith(JUnitParamsRunner.class)
public class ItemAllowedStatusesSchemaTest extends ApiTests {

  @Test
  public void schemaAndEnumIsConsistent() throws IOException {
    final Set<String> enumAllowedItemStatuses = getItemStatusNameEnumAllowedItemStatuses();
    final Set<String> schemaAllowedItemStatuses = getSchemaAllowedItemStatuses();

    assertTrue(enumAllowedItemStatuses.size() > 0);
    assertTrue(schemaAllowedItemStatuses.size() > 0);

    assertEquals("Schema enum does not match ItemStatusName values",
      enumAllowedItemStatuses, schemaAllowedItemStatuses);
  }

  @Test
  @Parameters(method = "getItemStatusNameEnumAllowedItemStatuses")
  public void canCreateItemsWithAllStatuses(String itemStatus) throws Exception {
    final UUID holdingsId = createInstanceAndHolding();

    final IndividualResource createResponse = itemsClient.create(
      new ItemRequestBuilder()
        .forHolding(holdingsId)
        .canCirculate()
        .withStatus(itemStatus));

    assertThat(createResponse.getJson().getJsonObject("status").getString("name"),
      is(itemStatus));
  }

  private UUID createInstanceAndHolding() throws MalformedURLException,
    InterruptedException, ExecutionException, TimeoutException {

    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    return holdingsStorageClient
      .create(new HoldingRequestBuilder().forInstance(instance.getId()))
      .getId();
  }

  private Set<String> getSchemaAllowedItemStatuses() throws IOException {
    final String itemJson = new String(readAllBytes(get("ramls/item.json")),
      StandardCharsets.UTF_8);

    final JsonObject itemSchema = new JsonObject(itemJson);

    JsonArray allowedStatuses = itemSchema.getJsonObject("properties")
      .getJsonObject("status").getJsonObject("properties")
      .getJsonObject("name").getJsonArray("enum");

    return allowedStatuses.stream()
      .map(element -> (String) element)
      .collect(Collectors.toSet());
  }

  private Set<String> getItemStatusNameEnumAllowedItemStatuses() {
    return Arrays.stream(ItemStatusName.values())
      .map(ItemStatusName::value)
      .collect(Collectors.toSet());
  }
}
