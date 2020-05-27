package org.folio.inventory.domain.items;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.vertx.core.json.JsonObject;

import org.folio.inventory.domain.items.ItemStatusName;

public class ItemTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void cannotCreateItemIfStatusIsNull() {
    expectedException.expect(instanceOf(NullPointerException.class));
    expectedException.expectMessage("Status is required");

    new Item("id", "holding-id", null, "material-type-id",
      "permanent-loan-type-id", new JsonObject());
  }

}
