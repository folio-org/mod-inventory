package org.folio.inventory.resources;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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

  @Test
  public void jsonWithoutNullValuesOnly() {
    var testValue = "https://test.com";
    var electronicAccessKey = "electronicAccess";
    var item = new Item(UUID.randomUUID().toString(), null, null,
      new Status(ItemStatusName.AVAILABLE), null, null, null);
    var electronicAccess = new ElectronicAccess(testValue, "", null, null, null);
    item.withElectronicAccess(List.of(electronicAccess));
    var json = new ItemRepresentation()
      .toJson(item, null, new JsonObject().put("contributors", new JsonArray()), null, null, null, null, null, null);
    var electronicAccessObject = json.getJsonArray(electronicAccessKey);

    var nullableList = new ArrayList<ElectronicAccess>();
    nullableList.add(null);
    item.withElectronicAccess(nullableList);
    var nullElectronicAccessJson = new ItemRepresentation()
      .toJson(item, null, new JsonObject().put("contributors", new JsonArray()), null, null, null, null, null, null);
    var emptyElectronicAccessObject = nullElectronicAccessJson.getJsonArray(electronicAccessKey);

    assertThat(electronicAccessObject.size(), is(1));
    assertThat(electronicAccessObject.getJsonObject(0).fieldNames().size(), is(2));
    assertThat(electronicAccessObject.getJsonObject(0).getValue("uri"), is(testValue));
    assertThat(emptyElectronicAccessObject, is(new JsonArray()));
  }
}
