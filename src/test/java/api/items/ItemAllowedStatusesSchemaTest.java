package api.items;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.folio.inventory.domain.items.ItemStatusName;
import org.junit.jupiter.api.Test;

/**
 * This test verifies that ramls/item.json and {@link ItemStatusName} is consistent.
 * <p>
 * If you're introducing a new status for an item you have to update both item.json
 * and the {@link ItemStatusName} enum, otherwise item won't pass validation
 * and will be rejected.
 */
class ItemAllowedStatusesSchemaTest {

  @Test
  void schemaAndEnumIsConsistent() throws IOException {
    final Set<String> enumAllowedItemStatuses = getItemStatusNameEnumAllowedItemStatuses();
    final Set<String> schemaAllowedItemStatuses = getSchemaAllowedItemStatuses();

    assertFalse(enumAllowedItemStatuses.isEmpty());
    assertFalse(schemaAllowedItemStatuses.isEmpty());
    assertEquals(enumAllowedItemStatuses, schemaAllowedItemStatuses,
      "Schema enum does not match ItemStatusName values" + System.lineSeparator()
      + getDifferencesBetweenCollectionsMessage(enumAllowedItemStatuses, schemaAllowedItemStatuses));
  }

  private String getDifferencesBetweenCollectionsMessage(Set<String> coll1, Set<String> coll2) {
    StringBuilder result = new StringBuilder();
    List<String> resultList = new ArrayList<>(coll1);
    resultList.removeAll(coll2);
    result.append("Item list 1:");
    result.append(resultList);
    resultList = new ArrayList<>(coll2);
    resultList.removeAll(coll1);
    result.append(System.lineSeparator());
    result.append("Item List 2:");
    result.append(resultList);
    result.append(System.lineSeparator());
    return result.toString();
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
