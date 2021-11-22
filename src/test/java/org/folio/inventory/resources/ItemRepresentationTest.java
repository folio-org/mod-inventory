package org.folio.inventory.resources;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.net.URL;
import java.util.UUID;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.junit.Test;

public class ItemRepresentationTest {

  @Test
  public void jsonContainsVersion() throws Throwable {
    WebContext webContext = when(mock(WebContext.class).absoluteUrl(any()))
        .thenReturn(new URL("http://localhost")).getMock();
    JsonObject instance = new JsonObject().put("contributors", new JsonArray());
    Item item = new Item(UUID.randomUUID().toString(), "123", null,
        new Status(ItemStatusName.AVAILABLE), null, null, null);
    JsonObject json = new ItemRepresentation("items")
        .toJson(item, null, instance, null, null, null, null, null, null, webContext);
    assertThat(json.getString("_version"), is("123"));
  }

}
