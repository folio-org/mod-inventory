package org.folio.inventory.domain.items;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.vertx.core.json.JsonObject;

public class ItemTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void cannotCreateItemIfStatusIsNull() {
    expectedException.expect(instanceOf(NullPointerException.class));
    expectedException.expectMessage("Status is required");

    new Item("id", "holding-id", "6", null, "material-type-id",
      "permanent-loan-type-id", new JsonObject());
  }

  @Test
  public void versionIsPreserved() {
    var item = new Item("id", "5", "holding-id", new Status(ItemStatusName.AVAILABLE), "material-type-id",
        "permanent-loan-type-id", new JsonObject());
    assertThat(item.getVersion(), is("5"));
    item.changeStatus(ItemStatusName.AGED_TO_LOST);
    assertThat(item.getVersion(), is("5"));
    item = item.withBarcode("789");
    assertThat(item.getVersion(), is("5"));
    item = item.copyWithNewId("foo");  // the copy is a new item without version
    assertThat(item.getVersion(), is(nullValue()));
  }
}
