package org.folio.inventory.support;

import static org.folio.inventory.domain.items.ItemStatusName.AVAILABLE;
import static org.folio.inventory.support.JsonHelper.getNestedProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.Status;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.UUID;

@RunWith(JUnit4.class)
public class ItemUtilTest {

  @Test
  public void shouldReturnItemAsMappingResultRepresentation() {
    // given
    Item item = new Item(UUID.randomUUID().toString(), "2", UUID.randomUUID().toString(),
      new Status(AVAILABLE), UUID.randomUUID().toString(), UUID.randomUUID().toString(), null)
      .withTemporaryLoanTypeId(UUID.randomUUID().toString())
      .withPermanentLocationId(UUID.randomUUID().toString())
      .withTemporaryLocationId(UUID.randomUUID().toString());

    // when
    String mappedResult = ItemUtil.mapToMappingResultRepresentation(item);
    JsonObject actualItemJson = new JsonObject(mappedResult);

    // then
    assertEquals(item.id, actualItemJson.getString("id"));
    assertEquals(item.getVersion(), actualItemJson.getString("_version"));
    assertEquals(item.getHoldingId(), actualItemJson.getString("holdingsRecordId"));
    assertEquals(item.getStatus().getName().value(), getNestedProperty(actualItemJson, "status", "name"));
    assertEquals(item.getMaterialTypeId(), getNestedProperty(actualItemJson, "materialType", "id"));
    assertEquals(item.getPermanentLoanTypeId(), getNestedProperty(actualItemJson, "permanentLoanType", "id"));
    assertEquals(item.getTemporaryLoanTypeId(), getNestedProperty(actualItemJson, "temporaryLoanType", "id"));
    assertEquals(item.getPermanentLocationId(), getNestedProperty(actualItemJson, "permanentLocation", "id"));
    assertEquals(item.getTemporaryLocationId(), getNestedProperty(actualItemJson, "temporaryLocation", "id"));
  }

  @Test
  public void shouldMapNestedFieldsAndParseVersionAndPassThroughOtherFields() {
    // given
    JsonObject patchJson = new JsonObject()
      .put("_version", "7")
      .put("materialType", new JsonObject().put("id", "mt-id"))
      .put("permanentLoanType", new JsonObject().put("id", "plt-id"))
      .put("temporaryLoanType", new JsonObject().put("id", "tlt-id"))
      .put("permanentLocation", new JsonObject().put("id", "pl-id"))
      .put("temporaryLocation", new JsonObject().put("id", "tl-id"))
      .put("barcode", "123456")
      .put("notes", "keep-as-is");

    // when
    JsonObject result = ItemUtil.patchToStorageJson(patchJson);

    // then
    assertEquals(Integer.valueOf(7), result.getInteger("_version"));

    assertEquals("mt-id", result.getString("materialTypeId"));
    assertEquals("plt-id", result.getString("permanentLoanTypeId"));
    assertEquals("tlt-id", result.getString("temporaryLoanTypeId"));
    assertEquals("pl-id", result.getString("permanentLocationId"));
    assertEquals("tl-id", result.getString("temporaryLocationId"));

    assertEquals("123456", result.getString("barcode"));
    assertEquals("keep-as-is", result.getString("notes"));

    Assert.assertNull(result.getValue("materialType"));
    Assert.assertNull(result.getValue("permanentLoanType"));
    Assert.assertNull(result.getValue("temporaryLoanType"));
    Assert.assertNull(result.getValue("permanentLocation"));
    Assert.assertNull(result.getValue("temporaryLocation"));
  }

  @Test
  public void shouldSetVersionNullWhenPatchVersionIsNull() {
    // given
    JsonObject patchJson = new JsonObject()
      .put("_version", (Object) null);

    // when
    JsonObject result = ItemUtil.patchToStorageJson(patchJson);

    // then
    Assert.assertTrue(result.containsKey("_version"));
    Assert.assertNull(result.getValue("_version"));
  }

  @Test
  public void shouldRemoveAllReadOnlyFieldsFromJson() {
    // given
    JsonObject itemJson = new JsonObject()
      .put("title", "Some title")
      .put("callNumber", "CN")
      .put("contributorNames", "Contrib")
      .put("effectiveShelvingOrder", "order")
      .put("effectiveCallNumberComponents", new JsonObject().put("callNumber", "abc"))
      .put("isBoundWith", true)
      .put("boundWithTitles", new io.vertx.core.json.JsonArray().add("t1"))
      .put("effectiveLocation", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("metadata", new JsonObject().put("createdDate", "2020-01-01"))
      // non read-only fields must stay
      .put("barcode", "123456")
      .put("id", UUID.randomUUID().toString());

    // when
    ItemUtil.removeReadOnlyFields(itemJson);

    // then
    assertFalse(itemJson.containsKey("title"));
    assertFalse(itemJson.containsKey("callNumber"));
    assertFalse(itemJson.containsKey("contributorNames"));
    assertFalse(itemJson.containsKey("effectiveShelvingOrder"));
    assertFalse(itemJson.containsKey("effectiveCallNumberComponents"));
    assertFalse(itemJson.containsKey("isBoundWith"));
    assertFalse(itemJson.containsKey("boundWithTitles"));
    assertFalse(itemJson.containsKey("effectiveLocation"));
    assertFalse(itemJson.containsKey("metadata"));

    assertEquals("123456", itemJson.getString("barcode"));
    assertNotNull(itemJson.getString("id"));
  }

  @Test
  public void shouldNotFailWhenReadOnlyFieldsAreMissing() {
    // given
    JsonObject itemJson = new JsonObject()
      .put("barcode", "b")
      .put("holdingsRecordId", UUID.randomUUID().toString());

    // when
    ItemUtil.removeReadOnlyFields(itemJson);

    // then
    assertEquals("b", itemJson.getString("barcode"));
    assertNotNull(itemJson.getString("holdingsRecordId"));
  }
}
