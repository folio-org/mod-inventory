package org.folio.inventory.domain.items;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class ItemTest {

  @Test
  void cannotCreateItemIfStatusIsNull() {
    var ex = assertThrows(NullPointerException.class, () ->
      new Item("id", "holding-id", "6", null, "material-type-id",
        "permanent-loan-type-id", null));
    assertTrue(ex.getMessage().contains("Status is required"));
  }

  @Test
  void versionIsPreserved() {
    var item = new Item("id", "5", "holding-id", new Status(ItemStatusName.AVAILABLE), "material-type-id",
      "permanent-loan-type-id", new JsonObject());
    assertEquals("5", item.getVersion());
    item.changeStatus(ItemStatusName.AGED_TO_LOST);
    assertEquals("5", item.getVersion());
    item = item.withBarcode("789");
    assertEquals("5", item.getVersion());
    item = item.copyWithNewId("foo");  // the copy is a new item without version
    assertNull(item.getVersion());
  }
}
