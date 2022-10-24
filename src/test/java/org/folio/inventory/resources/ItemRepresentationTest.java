package org.folio.inventory.resources;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.UUID;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.junit.Test;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class ItemRepresentationTest {
  @Test
  public void jsonContainsVersion() {
    JsonObject instance = new JsonObject().put("contributors", new JsonArray());
    Item item = new Item(UUID.randomUUID().toString(), "123", null,
        new Status(ItemStatusName.AVAILABLE), null, null, null);
    JsonObject json = new ItemRepresentation()
        .toJson(item, null, instance, null, null, null, null, null, null);
    assertThat(json.getString("_version"), is("123"));
  }
}
